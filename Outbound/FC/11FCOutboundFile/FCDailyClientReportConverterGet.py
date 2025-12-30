import os
import json
from collections import defaultdict
from datetime import datetime


class FCDailyClientReportConverterGet:
    """
    Converts inventory JSON (results-based) into Daily Client Inventory CSV.
    """

    def __init__(self, client_id="357"):
        self.client_id = client_id

        self.headers = [
            "Item",
            "Description",
            "Available",
            "Committed",
            "On-Hold",
            "Seconds",
            "Waste",
            "Staged",
            "Total"
        ]

    def convert(self, json_path, output_dir):
        # Load JSON
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

       
        results = data.get("results", [])

        if not results:
            print("[WARNING] No inventory records found.")
            return None

        # Aggregate packaged_amount per material
        material_totals = defaultdict(int)

        for rec in results:
            material = rec.get("material", "")
            qty = rec.get("packaged_amount", 0) or 0
            material_totals[material] += qty

        # Build rows
        rows = []
        for material, available_qty in material_totals.items():
            rows.append([
                material,          # Item
                "",                # Description (not in JSON)
                available_qty,     # Available
                0,                 # Committed
                0,                 # On-Hold
                0,                 # Seconds
                0,                 # Waste
                0,                 # Staged
                available_qty      # Total
            ])

        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "DAILY-CLIENT.csv")

        run_datetime = datetime.now().strftime("%m-%d-%Y %H:%M")

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                # Report header section
                f.write("Daily Client Inventory Report\n")
                f.write(f"Client: {self.client_id}\n")
                f.write(f"Run Date/Time,{run_datetime}\n")
                f.write("\n")

                # Column headers
                f.write(",".join(self.headers) + "\n")

                # Data rows
                for r in rows:
                    f.write(",".join(str(x) for x in r) + "\n")

            print(f"[SUCCESS] Conversion complete -- {len(rows)} records written to {output_path}")
            return output_path

        except Exception as e:
            print(f"[ERROR] Failed to write CSV file: {e}")
            raise


# Example usage
if __name__ == "__main__":
    converter = FCDailyClientReportConverterGet(client_id="357")
    converter.convert("inventory.json", "output")
