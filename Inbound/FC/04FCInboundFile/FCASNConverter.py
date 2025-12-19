import os
import json
from datetime import datetime


class ASNConverter:
    @staticmethod
    def _parse_fixed(text: str, start: int, end: int) -> str:
        """Safely slice a fixed-width substring (1-based inclusive)."""
        return text[start - 1:end].strip() if len(text) >= end else text[start - 1:].strip()

    @classmethod
    def convert(cls, input_path: str, output_dir: str) -> str:
        """Convert a fixed-width ASN file to JSON. Stops if any part number is missing."""
        # --- Read file ---
        with open(input_path, "r", encoding="utf-8") as f:
            lines = [line.rstrip("\n") for line in f if line.strip()]

        if len(lines) < 3:
            raise ValueError("File must have at least Transaction, Header, and one Line record")

        # --- Transaction Record ---
        tran_line = lines[0]
        transaction_name = cls._parse_fixed(tran_line, 1, 30)
        transaction_version = cls._parse_fixed(tran_line, 31, 45)

        # --- Header Record ---
        head_line = lines[1]
        segment_name = cls._parse_fixed(head_line, 1, 20)
        transaction_type = cls._parse_fixed(head_line, 21, 21)
        invoice_number = cls._parse_fixed(head_line, 22, 41)
        supplier_number = cls._parse_fixed(head_line, 42, 61)
        client_id = cls._parse_fixed(head_line, 62, 71)
        invoice_type = cls._parse_fixed(head_line, 72, 72)
        invoice_date = cls._parse_fixed(head_line, 73, 80)
        originator_ref = cls._parse_fixed(head_line, 81, 90)
        expected_receipt_date = cls._parse_fixed(head_line, 91, 98)

        # --- Prepare for detail lines ---
        detail_lines = lines[2:]
        order_lines = []
        error_lines = []

        for idx, dline in enumerate(detail_lines, start=3):
            segment_name = cls._parse_fixed(dline, 1, 20)
            line_number = cls._parse_fixed(dline, 21, 24)
            sub_line = cls._parse_fixed(dline, 25, 28)
            expected_qty = cls._parse_fixed(dline, 29, 38)
            part_number = cls._parse_fixed(dline, 39, 68)
            origin_code = cls._parse_fixed(dline, 69, 88)
            rev_level = cls._parse_fixed(dline, 89, 92)
            lotno = cls._parse_fixed(dline, 93, 112)
            to_host_account = cls._parse_fixed(dline, 113, 142)
            return_code = cls._parse_fixed(dline, 143, 146)

            # --- Validation: Part Number Missing ---
            if not part_number:
                error_lines.append(f"Line {idx}: Missing part number -> {dline}")

            order_lines.append({
                "line_number": line_number or "",
                "material": part_number or "",
                "vendor_lot": "",
                "lot": lotno or "",
                "packaging": "EA",
                "packaged_amount": int(expected_qty) if expected_qty.isdigit() else 0,
                "upc": "",
                "custom_fields": [],
                "child_lines": [],
                "order_id": ""
            })

        # --- If any missing part numbers, log and stop ---
        if error_lines:
            os.makedirs(output_dir, exist_ok=True)
            log_path = os.path.join(output_dir, "asn_error_log.txt")

            with open(log_path, "a", encoding="utf-8") as log_file:
                log_file.write(f"\n[{datetime.now().isoformat()}] Error in file: {os.path.basename(input_path)}\n")
                log_file.write("\n".join(error_lines))
                log_file.write("\n" + "-" * 80 + "\n")

            raise ValueError(f"Missing part number(s) detected. Logged in {log_path}")

        # --- Build JSON Output ---
        order = {
            "owner": "FC",
            "project": "FC",
            "order_class": "Ecom",
            "lookup": invoice_number,
            "status": "A",
            "owner_reference": originator_ref,
            "vendor_reference": supplier_number,
            "requested_delivery_date": cls._format_date(expected_receipt_date),
            "warehouse": "2301",
            "carrier": supplier_number,
            "carrier_service": supplier_number,
            "addresses": [],
            "order_lines": order_lines,
            "custom_fields": [],
            "shipments": []
        }

        json_data = {"orders": [order]}

        # --- Write JSON Output ---
        os.makedirs(output_dir, exist_ok=True)
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.json")

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2)

        return output_path

    @staticmethod
    def _format_date(date_str: str) -> str:
        """Convert YYYYMMDD → ISO8601."""
        try:
            if len(date_str) == 8 and date_str.isdigit():
                return datetime.strptime(date_str, "%Y%m%d").isoformat()
        except Exception:
            pass
        return datetime.now().isoformat()
