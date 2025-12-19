import os
import logging
import importlib.util
import inspect
import json
from typing import Optional
import requests
from datetime import datetime

from .sftp_client import SFTPClient
from .email_notifier import EmailNotifier
from .date_filter_updater import DateRangeUpdater


class OutboundProcessor:
    def __init__(self, cfg, local_sftp, db, email: EmailNotifier, oauth):
        self.cfg = cfg
        self.local_sftp = local_sftp
        self.db = db
        self.email = email
        self.oauth = oauth
        self.logger = logging.getLogger(__name__)

    # -------------------------
    # DYNAMIC CONVERTER LOADER
    # -------------------------
    def _load_converter(self, converter_spec: str):
        try:
            if ":" in converter_spec:
                path, cls_name = converter_spec.split(":", 1)
            else:
                path = converter_spec
                cls_name = None

            abspath = os.path.abspath(os.path.join(os.path.dirname(self.cfg.path), path))
            if not os.path.exists(abspath):
                raise FileNotFoundError(f"Converter file not found: {abspath}")

            spec = importlib.util.spec_from_file_location("conv_mod", abspath)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            if cls_name:
                conv_cls = getattr(mod, cls_name, None)
                if conv_cls is None:
                    raise AttributeError(f"Converter class {cls_name} not found in {abspath}")
                return conv_cls

            for attr in dir(mod):
                obj = getattr(mod, attr)
                if inspect.isclass(obj) and getattr(obj, "__module__", None) == mod.__name__ and hasattr(obj, "convert"):
                    return obj

            for attr in dir(mod):
                obj = getattr(mod, attr)
                if hasattr(obj, "convert"):
                    return obj

            raise RuntimeError(f"No converter class with 'convert' found in {abspath}")

        except Exception as e:
            self.logger.error(f"Error loading converter {converter_spec}: {e}", exc_info=True)
            raise

    # -------------------------
    def _generate_output_filename(self, partner: str, flow_name: str, flow: dict, fallback_name: str = None) -> str:
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file_name = flow.get("output_file_name")

        if output_file_name:
            return (
                output_file_name.format(datetime=dt)
                if "{datetime}" in output_file_name
                else output_file_name
            )

        if fallback_name:
            return fallback_name

        ext = flow.get("file_extension") or ".dat"
        if not ext.startswith("."):
            ext = "." + ext

        return f"{flow_name}_{dt}{ext}"

    # -------------------------
    def _build_api_url(self, api_cfg: dict) -> str:
        if not api_cfg:
            return ""

        if api_cfg.get("url"):
            return api_cfg["url"]

        endpoint = api_cfg.get("endpoint")
        if not endpoint:
            return ""

        base = self.cfg.data.get("api", {}).get("base_url") or self.cfg.get_global("base_url")
        return base.rstrip("/") + "/" + endpoint.lstrip("/") if base else endpoint

    # -------------------------
    @staticmethod
    def _file_has_data(p: str) -> bool:
        if not os.path.exists(p) or os.path.getsize(p) == 0:
            return False

        with open(p, "r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                if line.strip():
                    return True

        return False

    # ============================================================
    # MAIN PROCESS FUNCTION
    # ============================================================
    def process(self, partner: str, flow_name: str):
        flow = self.cfg.get_flow(partner, "outbound", flow_name)
        converter_spec = flow.get("converter")

        if not converter_spec:
            self.logger.error(f"[{partner}.{flow_name}] No converter configured")
            return

        conv_cls = self._load_converter(converter_spec)

        payload_file = flow.get("payload_file")
        if not payload_file:
            self.logger.error(f"[{partner}.{flow_name}] No payload_file configured")
            return

        payload_path = os.path.join(os.path.dirname(self.cfg.path), payload_file)

        if not os.path.exists(payload_path):
            self.logger.error(f"[{partner}.{flow_name}] Payload file not found: {payload_file}")
            return

        # ---------------------------------------------
        # NEW: UPDATE PAYLOAD DATES FOR OUTBOUND FLOW
        # ---------------------------------------------
        try:
            DateRangeUpdater.update_payload_file(payload_path)
            self.logger.info(f"[{partner}.{flow_name}] Updated payload with yesterday's date range")
        except Exception as e:
            self.logger.error(f"[{partner}.{flow_name}] Failed updating date range: {e}", exc_info=True)
            return

        # API CALL (if configured)
        json_files = [payload_path]
        api_cfg = flow.get("api")

        if api_cfg:
            url = self._build_api_url(api_cfg)
            self.logger.info(f"[{partner}.{flow_name}] Calling API {url}")

            headers = self.oauth.get_auth_headers() if self.oauth else {}
            headers.update(self.cfg.data.get("api", {}).get("headers", {}))
            headers.update(api_cfg.get("headers", {}))

            method = api_cfg.get("method", "GET").upper()

            try:
                with open(payload_path, "r", encoding="utf-8") as f:
                    payload_data = f.read()

                try:
                    payload_json = json.loads(payload_data)
                except Exception:
                    payload_json = None

                if method == "GET":
                    resp = requests.get(url, headers=headers, timeout=api_cfg.get("timeout", 30))
                else:
                    resp = requests.post(
                        url,
                        json=payload_json if payload_json else None,
                        data=None if payload_json else payload_data,
                        headers=headers,
                        timeout=api_cfg.get("timeout", 30),
                    )

                if resp.status_code != 200:
                    self.logger.error(f"[{partner}.{flow_name}] API call failed: {resp.status_code}")
                    return

                response_dir = os.path.join(os.path.dirname(self.cfg.path), flow.get("local_output_dir", "output"))
                os.makedirs(response_dir, exist_ok=True)

                response_path = os.path.join(response_dir, f"api_response_{flow_name}.json")

                with open(response_path, "w", encoding="utf-8") as f:
                    f.write(resp.text)

                json_files = [response_path]

            except Exception as e:
                self.logger.error(f"[{partner}.{flow_name}] API call failed: {e}", exc_info=True)
                return

        # Ensure output directory
        output_dir = os.path.join(os.path.dirname(self.cfg.path), flow.get("local_output_dir"))
        os.makedirs(output_dir, exist_ok=True)

        generated_files = []

        # -------------------------
        # CONVERT JSON FILES
        # -------------------------
        for jfp in json_files:
            try:
                converter = conv_cls() if isinstance(conv_cls, type) else conv_cls
                out_path = converter.convert(jfp, output_dir)

                if not out_path or not self._file_has_data(out_path):
                    self.logger.info(f"[{partner}.{flow_name}] Skipping empty or invalid file: {jfp}")
                    continue

                dest_name = self._generate_output_filename(partner, flow_name, flow, fallback_name=os.path.basename(out_path))
                dest_abs = os.path.join(output_dir, dest_name)

                if os.path.abspath(out_path) != os.path.abspath(dest_abs):
                    import shutil
                    shutil.copy2(out_path, dest_abs)

                uploaded = None

                if flow.get("use_sftp"):
                    partner_cfg = self.cfg.data.get("partners", {}).get(partner, {})
                    sftp_cfg = dict(partner_cfg.get("sftp", {}))
                    remote_dir = flow.get("sftp", {}).get("remote_output_dir")

                    sftp = SFTPClient(sftp_cfg)
                    try:
                        uploaded = sftp.upload(dest_abs, remote_dir, remote_name=dest_name)
                        uploaded = dest_abs
                    except Exception:
                        self.logger.warning(f"[{partner}.{flow_name}] SFTP upload failed, using local fallback")
                        uploaded = self.local_sftp.upload(dest_abs, remote_dir, remote_name=dest_name)
                else:
                    uploaded = dest_abs

                self.logger.info(f"[{partner}.{flow_name}] File processed: {dest_name}")

                if uploaded and os.path.exists(uploaded) and self._file_has_data(uploaded):
                    generated_files.append(uploaded)

            except Exception as e:
                self.logger.error(f"[{partner}.{flow_name}] Error processing {jfp}: {e}", exc_info=True)

        # -------------------------
        # FINAL EMAIL
        # -------------------------
        if generated_files:
            body_lines = [
                f"The outbound flow {partner}.{flow_name} completed successfully.",
                f"Total files generated: {len(generated_files)}",
                "",
                "Files:",
            ]
            body_lines.extend(generated_files)
            body = "\n".join(body_lines)

            try:
                self.email.send(
                    subject=f"Outbound Flow Completed: {partner}.{flow_name}",
                    body=body,
                    attachments=generated_files,
                )
                self.logger.info(
                    f"[{partner}.{flow_name}] Sent success email with {len(generated_files)} attachments"
                )
            except Exception as e:
                self.logger.warning(f"[{partner}.{flow_name}] Failed to send final email: {e}")
        else:
            self.logger.info(f"[{partner}.{flow_name}] No output files generated; email skipped")
