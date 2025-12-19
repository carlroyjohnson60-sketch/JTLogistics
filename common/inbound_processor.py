# inbound_processor.py
import os
import shutil
import importlib.util
import inspect
import json
import logging
from typing import List, Any, Optional
import requests
import posixpath
import time
import traceback

from .sftp_client import SFTPClient
from .splitter import FileSplitter
from .email_notifier import EmailNotifier


class InboundProcessor:
    def __init__(self, cfg, local_sftp, db, email: EmailNotifier, oauth):
        self.cfg = cfg
        self.local_sftp = local_sftp
        self.db = db
        self.email = email
        self.oauth = oauth
        self.splitter = FileSplitter()
        self.logger = logging.getLogger(__name__)

    # ---------------------- Converter Helpers ---------------------- #
    def _load_converter(self, converter_spec: str):
        try:
            if ':' in converter_spec:
                path, cls_name = converter_spec.split(':', 1)
            else:
                path = converter_spec
                cls_name = None

            abspath = os.path.abspath(os.path.join(os.path.dirname(self.cfg.path), path))
            if not os.path.exists(abspath):
                raise FileNotFoundError(f"Converter file not found: {abspath}")

            spec = importlib.util.spec_from_file_location('conv_mod', abspath)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            if cls_name:
                conv_obj = getattr(mod, cls_name, None)
                if conv_obj is None:
                    raise AttributeError(f"Converter class/function {cls_name} not found in {abspath}")
                return conv_obj

            # Prefer classes with 'convert' method defined in module
            for attr in dir(mod):
                obj = getattr(mod, attr)
                if inspect.isclass(obj) and getattr(obj, '__module__', None) == mod.__name__ and hasattr(obj, 'convert'):
                    return obj
            # Next any object/function with 'convert'
            for attr in dir(mod):
                obj = getattr(mod, attr)
                if hasattr(obj, 'convert'):
                    return obj
            # Finally any callable
            for attr in dir(mod):
                obj = getattr(mod, attr)
                if callable(obj) and getattr(obj, '__module__', None) == mod.__name__:
                    return obj

            raise RuntimeError(f"No converter found in {abspath}")
        except Exception as e:
            self.logger.error(f"Error loading converter {converter_spec}: {e}", exc_info=True)
            raise

    def _ensure_converter_instance(self, conv_obj):
        # class -> instantiate if possible
        if inspect.isclass(conv_obj):
            try:
                instance = conv_obj()
                if hasattr(instance, 'convert') and callable(getattr(instance, 'convert')):
                    return instance
            except TypeError:
                # constructor required args: treat class as callable/static
                pass

        # object with convert -> return
        if hasattr(conv_obj, 'convert') and callable(getattr(conv_obj, 'convert')):
            return conv_obj

        # function -> wrap
        if callable(conv_obj):
            class FuncWrapper:
                def __init__(self, f):
                    self._f = f
                def convert(self, input_path, out_dir):
                    return self._f(input_path, out_dir)
            return FuncWrapper(conv_obj)

        raise RuntimeError("Unsupported converter type; expected class/instance/function with convert.")

    def _resolve_converter_output(self, result) -> List[str]:
        """
        Normalize converter return into list of absolute json file paths.
        Accepts: single path, list/tuple, or a directory (collect .json inside).
        """
        resolved: List[str] = []
        if not result:
            return resolved

        if isinstance(result, (list, tuple)):
            for p in result:
                if p:
                    resolved.append(os.path.abspath(p))
            return resolved

        candidate = str(result)
        if os.path.isdir(candidate):
            for fname in os.listdir(candidate):
                if fname.lower().endswith('.json'):
                    resolved.append(os.path.abspath(os.path.join(candidate, fname)))
            return resolved

        resolved.append(os.path.abspath(candidate))
        return resolved

    # ---------------------- Helpers ---------------------- #
    def _normalize_local_paths(self, file_list: List[str], in_tmp: str) -> List[str]:
        normalized = []
        for f in file_list or []:
            if not f:
                continue
            if os.path.isabs(f) and os.path.exists(f):
                normalized.append(f)
            else:
                candidate = os.path.join(in_tmp, os.path.basename(f))
                normalized.append(candidate)
        return normalized

    def _matches_start_pattern(self, filename: str, flow_cfg: dict) -> bool:
        pattern = flow_cfg.get('start_pattern')
        if not pattern:
            return True
        try:
            return (filename or "").strip().lower().startswith(str(pattern).strip().lower())
        except Exception:
            return True

    def _split_fixed_width_by_field(self, file_path: str, start: int, end: int, out_dir: str) -> List[str]:
        return self.splitter.split_by_field(file_path, start, end, out_dir)

    def _build_api_url(self, api_cfg: dict) -> str:
        if not api_cfg:
            return ''
        # use explicit base_url (global) plus endpoint
        base = self.cfg.data.get('api', {}).get('base_url') or self.cfg.get_global('base_url')
        if api_cfg.get('url'):
            return api_cfg.get('url')
        endpoint = api_cfg.get('endpoint', '')
        if not endpoint:
            return base or ''
        if base:
            return base.rstrip('/') + '/' + endpoint.lstrip('/')
        return endpoint

    def _move_sftp_file(self, sftp: SFTPClient, remote_input: str, filename: str, success: bool):
        dest_dir = "/Incoming/Archives" if success else "/Incoming/DeadLetter"
        remote_src = posixpath.join(remote_input, filename)

        try:
            base_dir = os.path.abspath(self.cfg.get_global('working_dir', os.path.dirname(self.cfg.path)))
            tmp_dir = os.path.abspath(self.cfg.get_global('tmp_dir', os.path.join(base_dir, 'tmp')))
            os.makedirs(tmp_dir, exist_ok=True)
            local_tmp = os.path.join(tmp_dir, filename)

            if not os.path.exists(local_tmp):
                self.logger.info("Local tmp %s not found; attempting to download %s", local_tmp, remote_src)
                downloaded = sftp.download(remote_src, tmp_dir)
                if not downloaded or not os.path.exists(local_tmp):
                    raise FileNotFoundError(f"Local file not found after download attempt: {local_tmp}")

            uploaded_remote = sftp.upload(local_tmp, dest_dir, remote_name=filename)
            self.logger.info("Uploaded %s -> %s", local_tmp, uploaded_remote)

            try:
                sftp.remove(remote_src)
            except Exception as e:
                self.logger.warning("Failed to remove source %s; will attempt rename: %s", remote_src, e)
                try:
                    backup_remote = posixpath.join(dest_dir, filename)
                    sftp.rename(remote_src, backup_remote)
                    self.logger.info("Renamed remote %s -> %s", remote_src, backup_remote)
                except Exception as e2:
                    self.logger.error("Failed to rename remote %s -> %s: %s", remote_src, dest_dir, e2)

            try:
                if os.path.exists(local_tmp):
                    os.remove(local_tmp)
                    self.logger.debug("Removed local tmp file %s", local_tmp)
            except Exception:
                pass

            self.logger.info("Moved %s to %s", filename, dest_dir)

        except Exception as e:
            self.logger.error("Failed to move %s to %s: %s", filename, dest_dir, e, exc_info=True)
            print(f"Failed to move {filename} to {dest_dir}: {e}")

    def _is_fresh_file(self, path: str, max_age_seconds: int = 30) -> bool:
        try:
            mtime = os.path.getmtime(path)
            return (time.time() - mtime) <= max_age_seconds
        except Exception:
            return False

    def _find_order_id(self, obj: Any) -> Optional[str]:
        """
        Try to find an 'order_id' in the JSON structure recursively.
        Returns first found value as string, or None.
        """
        if obj is None:
            return None
        if isinstance(obj, dict):
            if 'order_id' in obj:
                return str(obj['order_id'])
            # try common alternatives
            for k in ('owner_reference', 'lookup', 'reference', 'orderNumber', 'order_number'):
                if k in obj and obj.get(k):
                    return str(obj.get(k))
            for v in obj.values():
                found = self._find_order_id(v)
                if found:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = self._find_order_id(item)
                if found:
                    return found
        return None

    # --------------------------- MAIN PROCESS --------------------------- #
    def process(self, partner: str, flow_name: str):
        try:
            flow = self.cfg.get_flow(partner, 'inbound', flow_name)
            conv_obj = self._load_converter(flow.get('converter'))
            conv = self._ensure_converter_instance(conv_obj)

            base_dir = os.path.abspath(self.cfg.get_global('working_dir', os.path.dirname(self.cfg.path)))
            tmp_dir = os.path.abspath(self.cfg.get_global('tmp_dir', os.path.join(base_dir, 'tmp')))
            in_tmp = os.path.join(tmp_dir, f"{partner}_{flow_name}_in")
            os.makedirs(in_tmp, exist_ok=True)

            sftp = None
            remote_input = None
            files: List[str] = []

            # Download files (SFTP or local)
            if flow.get('use_sftp'):
                partner_cfg = self.cfg.data.get('partners', {}).get(partner, {})
                sftp_cfg = dict(partner_cfg.get('sftp', {}))
                remote_input = flow.get('sftp', {}).get('remote_input_dir')
                if not remote_input:
                    raise ValueError(f"No remote_input_dir configured for {partner}.{flow_name}")
                self.logger.info("[%s.%s] Connecting to SFTP server...", partner, flow_name)
                sftp = SFTPClient(sftp_cfg)
                files = sftp.download(remote_input, in_tmp)
            else:
                local_dir = flow.get('local_input_dir')
                self.logger.info("[%s.%s] Using local files from %s", partner, flow_name, local_dir)
                files = self.local_sftp.download(local_dir, in_tmp)

            files = self._normalize_local_paths(files, in_tmp)

            # Filter by start pattern
            matched_files = [f for f in files if self._matches_start_pattern(os.path.basename(f), flow)]
            if not matched_files:
                msg = f"[{partner}.{flow_name}] ERROR: No files found matching start_pattern '{flow.get('start_pattern')}'"
                self.logger.error(msg)
                print(msg)
                return

            split_config = flow.get('split', {}) or {}

            for fp in matched_files:
                filename = os.path.basename(fp)
                success = True
                splits = [fp]
                created_split_out = None
                http_status_ok = True
                email_rows: List[dict] = []
                full_response_text = ""

                try:
                    # Perform splitting if configured
                    if split_config.get('enabled'):
                        st = int(split_config.get('field_start'))
                        ed = int(split_config.get('field_end'))
                        split_out = os.path.join(in_tmp, 'splits')
                        os.makedirs(split_out, exist_ok=True)
                        splits = self._split_fixed_width_by_field(fp, st, ed, split_out)
                        created_split_out = split_out

                    # For each piece, convert and post
                    for piece in splits:
                        try:
                            out_json_dir = os.path.join(base_dir, flow.get('output_json_dir', ''))
                            os.makedirs(out_json_dir, exist_ok=True)

                            # Convert piece -> converter may return path(s), list or directory
                            convert_result = conv.convert(piece, out_json_dir)
                            json_paths = self._resolve_converter_output(convert_result)

                            if not json_paths:
                                raise RuntimeError(f"Converter returned no JSON for piece {piece}")

                            # For each converted json for this piece
                            for json_path in json_paths:
                                if not os.path.exists(json_path):
                                    raise FileNotFoundError(f"Converter output file not found: {json_path}")

                                # --- Read the exact converted JSON text from disk and log it ---
                                with open(json_path, 'r', encoding='utf-8') as jf:
                                    payload_text = jf.read()

                                # log for debugging: path + preview
                                self.logger.debug("[%s.%s] Will POST JSON from %s (preview first 2000 chars): %s",
                                                  partner, flow_name, json_path, (payload_text[:2000] if payload_text else ''))

                                # Try to parse JSON
                                payload_obj = None
                                try:
                                    payload_obj = json.loads(payload_text)
                                except Exception:
                                    payload_obj = None

                                # determine order id by scanning payload
                                order_id = self._find_order_id(payload_obj) or self._find_order_id(payload_obj.get('orders')[0] if (isinstance(payload_obj, dict) and payload_obj.get('orders')) else None) if isinstance(payload_obj, dict) else None

                                # POST to API (with retry)
                                api_cfg = flow.get('api') or {}
                                response_text = ""
                                status_code = None

                                if api_cfg:
                                    url = self._build_api_url(api_cfg)
                                    # build headers: auth headers then flow headers override
                                    headers = {}
                                    try:
                                        headers = self.oauth.get_auth_headers() or {}
                                    except Exception:
                                        headers = {}
                                    # Merge flow headers
                                    flow_headers = api_cfg.get('headers') or {}
                                    if isinstance(flow_headers, dict):
                                        for hk, hv in flow_headers.items():
                                            headers[str(hk)] = str(hv)

                                    # Determine sending strategy
                                    max_attempts = api_cfg.get('retry', {}).get('max_attempts', 1)
                                    delay_seconds = api_cfg.get('retry', {}).get('delay_seconds', 1)
                                    attempt = 0
                                    last_exception = None

                                    while attempt < max_attempts:
                                        attempt += 1
                                        try:
                                            # If we parsed JSON -> send as json to keep structure
                                            if payload_obj is not None:
                                                resp = requests.post(url, json=payload_obj, headers=headers, timeout=api_cfg.get('timeout', 30))
                                            else:
                                                # send raw text
                                                if not any(k.lower() == 'content-type' for k in (headers.keys() or [])):
                                                    headers['Content-Type'] = 'application/json'
                                                resp = requests.post(url, data=payload_text.encode('utf-8'), headers=headers, timeout=api_cfg.get('timeout', 30))

                                            status_code = getattr(resp, 'status_code', None)
                                            response_text = resp.text
                                            # consider 2xx success
                                            if getattr(resp, 'ok', False):
                                                break
                                            else:
                                                last_exception = Exception(f"HTTP {status_code}: {response_text[:2000]}")
                                                self.logger.warning("API returned non-2xx (attempt %s/%s) for %s: %s", attempt, max_attempts, json_path, status_code)
                                        except Exception as post_err:
                                            last_exception = post_err
                                            self.logger.warning("API post attempt %s/%s failed for %s: %s", attempt, max_attempts, json_path, post_err, exc_info=True)
                                        if attempt < max_attempts:
                                            time.sleep(delay_seconds)

                                    if last_exception and not (status_code and 200 <= status_code < 300):
                                        http_status_ok = False
                                        # if no response_text captured, record exception text
                                        if not response_text:
                                            response_text = str(last_exception)

                                # Save API response for traceability
                                proc_dir = os.path.join(base_dir, flow.get('local_processed_dir', ''))
                                response_dir = os.path.join(proc_dir, 'response')
                                os.makedirs(response_dir, exist_ok=True)
                                timestamp = int(time.time())
                                safe_json_name = os.path.basename(json_path)
                                rpath = os.path.join(response_dir, f"{safe_json_name}.{timestamp}.response.txt")
                                try:
                                    with open(rpath, 'w', encoding='utf-8') as rf:
                                        rf.write(f"status_code: {status_code}\n\n{response_text}")
                                except Exception as write_err:
                                    self.logger.error("Failed to write response file %s: %s", rpath, write_err, exc_info=True)

                                # Prepare row for email (split vs non-split)
                                if split_config.get('enabled'):
                                    email_rows.append({
                                        'order_id': order_id or '-',
                                        'status': 'Success' if status_code and 200 <= status_code < 300 else 'Failed',
                                        'response': 'Success' if status_code and 200 <= status_code < 300 else (response_text[:2000] if response_text else '')
                                    })
                                else:
                                    if not (status_code and 200 <= status_code < 300):
                                        full_response_text += f"Converted JSON file: {safe_json_name}\nHTTP status: {status_code}\nResponse:\n{response_text}\n\n"

                        except Exception as piece_err:
                            # log & mark failure for this piece
                            self.logger.error("Error processing piece %s: %s", os.path.basename(piece), piece_err, exc_info=True)
                            http_status_ok = False
                            if split_config.get('enabled'):
                                email_rows.append({'order_id': '-', 'status': 'Failed', 'response': str(piece_err)[:2000]})
                        finally:
                            # Remove piece file if it lives in the split_out dir
                            try:
                                if created_split_out and os.path.commonpath([os.path.abspath(piece), os.path.abspath(created_split_out)]) == os.path.abspath(created_split_out):
                                    if os.path.exists(piece):
                                        os.remove(piece)
                            except Exception:
                                pass

                    # end for pieces
                    success = http_status_ok

                except Exception as fp_err:
                    self.logger.error("Error processing %s: %s", filename, fp_err, exc_info=True)
                    success = False

                finally:
                    # move original remote file to Archives or DeadLetter
                    if flow.get('use_sftp') and sftp and remote_input:
                        try:
                            self._move_sftp_file(sftp, remote_input, filename, success)
                        except Exception as e:
                            self.logger.error("Failed moving remote file %s: %s", filename, e, exc_info=True)

                # ----------------- EMAIL ----------------- #
                if self.email and getattr(self.email, 'enabled', False):
                    try:
                        if split_config.get('enabled') and email_rows:
                            # send HTML table
                            html_body = EmailNotifier.format_html_table(email_rows)
                            self.email.send(subject=f"[{partner}.{flow_name}] Processed Split File {filename}", body=html_body, html=True)
                        elif not split_config.get('enabled') and not success:
                            # non-split failure: full response in email body
                            text_body = f"File: {filename}\n\nAPI Response(s):\n{full_response_text}"
                            self.email.send(subject=f"[{partner}.{flow_name}] Failed File {filename}", body=text_body)
                    except Exception as email_err:
                        self.logger.error("Failed to send email for %s: %s", filename, email_err, exc_info=True)

            # cleanup split folder
            if split_config.get('enabled'):
                split_out = os.path.join(in_tmp, 'splits')
                if os.path.exists(split_out):
                    try:
                        shutil.rmtree(split_out, ignore_errors=True)
                    except Exception:
                        pass

        except Exception as e:
            self.logger.error("[%s.%s] Fatal error: %s", partner, flow_name, e, exc_info=True)
            print(f"[{partner}.{flow_name}] Fatal error: {e}")
            raise
