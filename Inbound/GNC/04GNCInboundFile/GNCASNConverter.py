import csv
import json
from datetime import datetime

class ASNConverter:
    def __init__(self, file_path):
        self.file_path = file_path

    def convert(self):
        order_lines = []
        with open(self.file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for idx, row in enumerate(reader, start=1):
                if not row or len(row) < 10:
                    continue

                # Extract data based on your example structure
                client = row[0].strip('"')
                material = row[1].strip('"')
                description = row[2].strip('"')
                quantity = float(row[3]) if row[3] else 0
                packaging = row[7].strip('"')
                unitlen = float(row[8]) if row[8] else 0
                invdate = row[15].strip('"') if len(row) > 15 else ""
                invoice_date = row[16].strip('"') if len(row) > 16 else ""
                invoice_type = row[17].strip('"') if len(row) > 17 else ""

                # Build order line record
                order_lines.append({
                    "line_number": idx,
                    "material": material,
                    "vendor_lot": "",
                    "lot": "",
                    "packaging": "", #packaging,
                    "packaged_amount": quantity,
                    "upc": "",
                    "custom_fields": ["""
                        {"name": "description", "value": description},
                        {"name": "weight", "value": weight},
                        {"name": "invoice_type", "value": invoice_type}
                    """],
                    "child_lines": [],
                    "order_id": 0
                })

        # Create single order record similar to ASN structure
        order = {
            "owner": "GNC",
            "project": "GNC",
            "order_class": "Ecom",
            "lookup": client,
            "status": "A",
            "owner_reference": client,
            "vendor_reference": "GNC",
            "requested_delivery_date": self._format_date(invoice_date),
            "warehouse": "2301",
            "carrier": "GNC",
            "carrier_service": "GNC",
            "addresses": [],
            "order_lines": order_lines,
            "custom_fields": ["""
                {"name": "invoice_date", "value": invoice_date}
            """],
            "shipments": []
        }

        return {"orders": [order]}

    def _format_date(self, date_str):
        """Convert YYYYMMDD -> ISO8601"""
        try:
            if len(date_str) == 8 and date_str.isdigit():
                return datetime.strptime(date_str, "%Y%m%d").isoformat()
        except Exception:
            pass
        return datetime.now().isoformat()


# --- Example usage ---
"""if __name__ == "__main__":
    converter = ASNConverter("gnc_inventory.csv")
    json_data = converter.convert()

    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)

    print("✅ JSON created successfully as output.json")"""
