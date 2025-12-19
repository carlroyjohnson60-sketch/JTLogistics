import os
from datetime import datetime
from common.converters import CSVConverter


class GNCJsonToDatConverterSalesOrderGet(CSVConverter):
    def __init__(self):
        headers = ["TRNDTE", "CLIENT_ID", "INVNUM", "PRTNUM", "RCVQTY", "RCVUOM", "INVSTS"]
        super().__init__(headers)

    def _is_valid_line(self, line):
      
        if not line:
            return False

        status = line.get("status", "")
        packaged_amount = line.get("packaged_amount", 0)

        return status in ("Completed") and float(packaged_amount) > 0

    def map_row(self, order, line):
        """Map an order line to DAT file fields."""

        created_on = order.get("created_on", "")
        invsts = "A"
        client_id = "MRS"
        trndte = ""               # Column 1 → YYYYMMDDHHMMSS
        inv_date_mmddyyyy = ""    # Column 3 → MMDDYYYY

        if created_on:
            try:
                dt = datetime.fromisoformat(created_on.replace("Z", "+00:00"))

                # Full timestamp
                trndte = dt.strftime("%Y%m%d%H%M%S")

                # Date only in MMDDYYYY
                inv_date_mmddyyyy = dt.strftime("%m%d%Y")

            except Exception:
                trndte = created_on
                inv_date_mmddyyyy = ""

        if not line:
            return {
                "TRNDTE": trndte,
                "CLIENT_ID": client_id,
                "INVNUM": inv_date_mmddyyyy,
                "PRTNUM": "",
                "RCVQTY": 0,
                "RCVUOM": "EA",
                "INVSTS": invsts,
            }

        prtnum = self.safe_str(line.get("material", ""))
        rcvqty = int(line.get("packaged_amount", 0))

        return {
            "TRNDTE": trndte,
            "CLIENT_ID": client_id,
            "INVNUM": inv_date_mmddyyyy,
            "PRTNUM": prtnum,
            "RCVQTY": rcvqty,
            "RCVUOM": "EA",
            "INVSTS": invsts,
        }

    def convert(self, json_path: str, output_dir: str) -> str:
        """Convert JSON to .dat file for GNC format."""
        data = self._load_json(json_path)
        orders = data.get("orders", [])

        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "output.dat")

        if not orders:
            print("⚠️ No orders found in input JSON — generating empty DAT file.")
            with open(output_path, "w", encoding="utf-8") as f:
                pass   # create empty file
            return output_path

        rows = []
        for order in orders:
            order_lines = order.get("order_lines", [])
            if not order_lines:
                rows.append(self.map_row(order, None))
            else:
                for line in order_lines:
                    if self._is_valid_line(line):
                        rows.append(self.map_row(order, line))

        try:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                for row in rows:
                    line_parts = []
                    for idx, col in enumerate(self.headers):
                        value = row[col]
                        if idx == 4:  # RCVQTY
                            line_parts.append(str(int(value)))
                        else:
                            line_parts.append(f"\"{value}\"")

                    f.write(",".join(line_parts) + "\n")


            print(f"[SUCCESS] GNC Conversion complete: {len(rows)} records written to {output_path}")
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
    converter = GNCJsonToDatConverterSalesOrderGet()
    dat_path = converter.convert("gnc_input.json", "output")
    if dat_path:
        print(f"GNC DAT file created at: {dat_path}")
