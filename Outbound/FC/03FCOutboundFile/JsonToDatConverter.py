import os
from datetime import datetime
from common.converters import CSVConverter


class JsonToDatConverter(CSVConverter):
    """
    Converts JSON orders into .dat file with comma-separated values (no header).
    Only includes order lines where:
        - line['status'] == 'Completed'
        - line['packaged_amount'] > 0

    Format per line:
    TRNDTE,TRNTIME,TRNSEQ,PRTNUM,RCVQTY,RCVUOM,INVNUM,TRNTYP
    Example:
    20241107,113905,10014221,78229,940,EA,UO10005644,REC
    """

    def __init__(self):
        headers = ["TRNDTE", "TRNTIME", "TRNSEQ", "PRTNUM", "RCVQTY", "RCVUOM", "INVNUM", "TRNTYP"]
        super().__init__(headers)

    def map_row(self, order, line):
        """Map an order line to DAT file fields."""
        lookup = self.safe_str(order.get("lookup", ""))
        owner_reference = self.safe_str(order.get("owner_reference", ""))
        created_on = order.get("created_on", "")
        trntyp = "REC"
        rcvuom = "EA"

        # Parse date/time
        trndte, trntime = "", ""
        if created_on:
            try:
                dt = datetime.fromisoformat(created_on.replace("Z", "+00:00"))
                trndte = dt.strftime("%Y%m%d")
                trntime = dt.strftime("%H%M%S")
            except Exception:
                pass

        # Handle empty order line (shouldn't happen here)
        if not line:
            return None

        prtnum = self.safe_str(line.get("material", ""))
        rcvqty = line.get("packaged_amount", 0)

        return {
            "TRNDTE": trndte,
            "TRNTIME": trntime,
            "TRNSEQ": lookup,
            "PRTNUM": prtnum,
            "RCVQTY": int(rcvqty),
            "RCVUOM": rcvuom,
            "INVNUM": owner_reference,
            "TRNTYP": trntyp
        }

    def _is_valid_line(self, line):
        """Return True only if status == 'Completed' and packaged_amount > 0."""
        if not line:
            return False
        status = line.get("status", "")
        packaged_amount = line.get("packaged_amount", 0)
        return status == "Completed" and packaged_amount > 0

    def convert(self, json_path: str, output_dir: str) -> str:
        """Convert JSON to .dat file with filtering logic."""
        data = self._load_json(json_path)
        orders = data.get("orders", [])

        if not orders:
            print("⚠️ No orders found in input JSON.")
            return None

        rows = []
        for order in orders:
            order_lines = order.get("order_lines", [])
            for line in order_lines:
                if self._is_valid_line(line):
                    mapped = self.map_row(order, line)
                    if mapped:
                        rows.append(mapped)

        if not rows:
            print("[WARNING] No valid order lines found after filtering.")
            return None

        os.makedirs(output_dir, exist_ok=True)
        # Use generic output filename - processor will rename with proper {datetime}
        output_path = os.path.join(output_dir, "output.dat")

        try:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                for row in rows:
                    line = ",".join(str(row[col]) for col in self.headers)
                    f.write(line + "\n")

            print(f"[SUCCESS] Conversion complete: {len(rows)} valid records written to {output_path}")
            return output_path
        except Exception as e:
            print(f"[ERROR] Error writing output file {output_path}: {e}")
            raise

    def safe_str(self, value):
        """Convert None or non-string to safe string."""
        if value is None:
            return ""
        return str(value).strip()


if __name__ == "__main__":
    converter = JsonToDatConverter()
    dat_path = converter.convert("input.json", "output")
    if dat_path:
        print(f"DAT file created at: {dat_path}")
