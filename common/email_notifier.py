# email_notifier.py
import smtplib
import os
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Optional


class EmailNotifier:
    def __init__(self, cfg: dict, error_logger=None):
        self.cfg = cfg or {}
        self.enabled = bool(self.cfg.get('enabled', False))
        self.logger = logging.getLogger(__name__)
        self.error_logger = error_logger
        self.logger.debug(f"EmailNotifier initialized. Enabled={self.enabled}, Config keys={list(self.cfg.keys())}")

    def send(self, subject: str, body: str, to_addrs: Optional[List[str]] = None,
             attachments: Optional[List[str]] = None, html: bool = False) -> bool:
        """
        Send an email.
        - body: plain text or HTML (if html=True)
        - attachments: list of file paths
        """
        if not self.enabled:
            self.logger.debug("Email sending is disabled in configuration.")
            return False

        try:
            smtp_host = self.cfg.get('smtp_host')
            smtp_port = int(self.cfg.get('smtp_port', 25))
            from_addr = self.cfg.get('from') or self.cfg.get('from_addr') or self.cfg.get('sender')
            to_addrs = to_addrs or self.cfg.get('to')

            if not smtp_host or not from_addr or not to_addrs:
                raise ValueError("Email configuration incomplete: 'smtp_host', 'from', or 'to' missing.")

            if isinstance(to_addrs, str):
                to_addrs = [addr.strip() for addr in to_addrs.split(',') if addr.strip()]

            msg = MIMEMultipart()
            msg['From'] = from_addr
            msg['To'] = ", ".join(to_addrs)
            msg['Subject'] = subject or ''

            if html:
                msg.attach(MIMEText(body or "", 'html'))
            else:
                msg.attach(MIMEText(body or "", 'plain'))

            # Attach files if provided
            if attachments:
                if not isinstance(attachments, list):
                    attachments = [attachments]
                for fp in attachments:
                    if not fp or not os.path.exists(fp):
                        self.logger.warning(f"Attachment skipped (not found): {fp}")
                        continue
                    try:
                        self._attach_file(msg, fp)
                        self.logger.debug(f"Attached file: {fp}")
                    except Exception as ex:
                        self.logger.warning(f"Failed to attach file {fp}: {ex}")
                        if self.error_logger:
                            try:
                                self.error_logger.log_error(f"Failed to attach file {fp}", exc_info=True)
                            except Exception:
                                pass

            self.logger.debug(f"Connecting to SMTP {smtp_host}:{smtp_port}")
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                try:
                    server.starttls()
                except Exception:
                    self.logger.debug("STARTTLS failed or unsupported; continuing without TLS")
                server.send_message(msg)

            self.logger.info(f"Email sent successfully to {to_addrs}, subject='{subject}'")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send email '{subject}': {e}", exc_info=True)
            if self.error_logger:
                try:
                    self.error_logger.log_error(f"Failed to send email '{subject}'", exc_info=True)
                except Exception:
                    pass
            return False

    @staticmethod
    def _attach_file(msg: MIMEMultipart, file_path: str):
        file_name = os.path.basename(file_path)
        with open(file_path, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{file_name}"')
        msg.attach(part)

    @staticmethod
    def format_html_table(rows: List[dict]) -> str:
        """
        Convert a list of dictionaries (rows) to an HTML table.
        Expected keys: order_id, status, response (but function tolerates other keys).
        """
        if not rows:
            return "<p>No results</p>"

        # Header order: prefer known names
        headers = ['order_id', 'status', 'response']
        if rows and set(rows[0].keys()) != set(headers):
            headers = list(rows[0].keys())

        html = [
            '<html><body>',
            '<p>Processing results:</p>',
            '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;">',
            '<thead style="background:#f2f2f2;"><tr>'
        ]
        for h in headers:
            html.append(f'<th>{h}</th>')
        html.append('</tr></thead><tbody>')

        for r in rows:
            html.append('<tr>')
            for h in headers:
                cell = r.get(h, '') or ''
                # minimal escaping
                cell = str(cell).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                html.append(f'<td>{cell}</td>')
            html.append('</tr>')
        html.append('</tbody></table></body></html>')
        return ''.join(html)
