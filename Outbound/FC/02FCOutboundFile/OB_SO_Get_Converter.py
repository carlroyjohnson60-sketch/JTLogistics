import json
import os
from datetime import datetime


class OrdersCSVConverter:
    FIELD_SPECS = [
        ("ASTSHIP", 20, "number", ""),   # Assist Shipment Number (from order.VC_SHIPID or similar)
        ("SUBNUM", 14, "text", ""),      # Package Number, e.g., DLx carton number
        ("ORDNUM", 20, "text", ""),      # Assist Order Number (order.owner_reference)
        ("ORDLIN", 4, "text", ""),       # Order Line Number (line number)
        ("PRTNUM", 30, "text", ""),      # Part number (line.material)
        ("SHPDTE", 8, "date", ""),       # Ship date MMDDYYYY
        ("TRAKNM", 30, "text", ""),      # Tracking Number
        ("WEIGHT", 10, "number", 0),     # Actual weight
        ("FRTRTE", 10, "number", 0),     # Actual cost (default 0)
        ("SHPQTY", 4, "number", 0),      # Shipment quantity
    ]

    @staticmethod
    def format_value(value, ftype):
        """Format values for CSV output."""
        if ftype == "date":
            if isinstance(value, datetime):
                return value.strftime("%m%d%Y")
            try:
                # Parse date string if given in ISO format
                dt = datetime.fromisoformat(str(value))
                return dt.strftime("%m%d%Y")
            except Exception:
                return datetime.now().strftime("%m%d%Y")
        elif ftype == "number":
            return str(value)
        else:
            return str(value).strip()

    @classmethod
    def build_record(cls, order, line):
        """Build one CSV line (no header)."""
        shipments = order.get("shipments", [])
        tracking = shipments[0].get("tracking_identifier", "") if shipments else ""

        values = []
        for name, length, ftype, default in cls.FIELD_SPECS:
            if name == "ASTSHIP":
                value = order.get("warehouse", default)
            elif name == "SUBNUM":
                value = line.get("packaging", default)
            elif name == "ORDNUM":
                value = order.get("owner_reference", default)
            elif name == "ORDLIN":
                value = str(line.get("line_number", "")).zfill(4)
            elif name == "PRTNUM":
                value = line.get("material", default)
            elif name == "SHPDTE":
                value = datetime.now()  # could be replaced with actual ship date
            elif name == "TRAKNM":
                value = tracking
            elif name == "WEIGHT":
                value = line.get("weight", default)
            elif name == "FRTRTE":
                value = line.get("cost", default)
            elif name == "SHPQTY":
                value = line.get("packaged_amount", default)
            else:
                value = default

            values.append(cls.format_value(value, ftype))
        return ",".join(values)

    @classmethod
    def convert(cls, json_path, output_dir):
        """Convert JSON order file to CSV file (no headers)."""
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        orders = data.get("orders", [])
        if not orders:
            raise ValueError("❌ No orders found in JSON")

        csv_lines = []
        for order in orders:
            for line in order.get("order_lines", []):
                csv_lines.append(cls.build_record(order, line))

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, f"shipment_{ts}.csv")

        with open(out_path, "w", encoding="utf-8", newline="") as f:
            f.write("\n".join(csv_lines))

        print(f"✅ CSV file created: {out_path}")
        return out_path


# 🔽 Direct test
if __name__ == "__main__":
    test_json = r"sample_order.json"   # path to your input JSON
    output_dir = r"output"

    try:
        converter = OrdersCSVConverter()
        out = converter.convert(test_json, output_dir)
        print("📦 CSV conversion successful:", out)
    except Exception as e:
        print(f"❌ Conversion failed: {e}")
