import os
import json
from collections import defaultdict


class GNCDailyClientReportConverterGet:
    """
    Converts inventory JSON (results-based) into GNC Daily Client CSV
    with Last Receipt Date and Pallet Count.
    """

    def __init__(self):
        self.headers = [
            "PRTNUM",
            "PRTFAM",
            "DESCRIPTION",
            "AVAILBLE",
            "COMMITTED",
            "HOLD",
            "SECONDS",
            "WASTE",
            "On hand",
            "LAST_RECEIPT_QUANTITY",
            "LAST_RECEIPT_DATE",
            "PALLET_COUNT"
        ]

    def convert(self, json_path, output_dir):
        # Load JSON
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        results = data.get("results", [])

        if not results:
            print("[WARNING] No inventory records found.")
            return None

        # Aggregations
        qty_by_material = defaultdict(int)
        pallets_by_material = defaultdict(set)

        for rec in results:
            material = rec.get("material", "")
            qty = rec.get("packaged_amount", 0) or 0
            pallet = rec.get("license_plate")

            qty_by_material[material] += qty
            if pallet:
                pallets_by_material[material].add(pallet)

        rows = []
        for material, available_qty in qty_by_material.items():
            pallet_count = len(pallets_by_material.get(material, []))

            rows.append([
                material,               # PRTNUM
                "MRS",                  # PRTFAM
                "",                     # DESCRIPTION
                available_qty,          # AVAILBLE
                0,                      # COMMITTED
                0,                      # HOLD
                0,                      # SECONDS
                0,                      # WASTE
                available_qty,          # On hand
                available_qty,          # LAST_RECEIPT_QUANTITY
                "",                     # LAST_RECEIPT_DATE
                pallet_count            # PALLET_COUNT
            ])

        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "GNC_DAILY_CLIENT.csv")

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                # Write header
                f.write(",".join(self.headers) + "\n")

                # Write data rows
                for r in rows:
                    f.write(",".join(str(x) for x in r) + "\n")

            print(f"[SUCCESS] GNC report generated — {len(rows)} records written to {output_path}")
            return output_path

        except Exception as e:
            print(f"[ERROR] Failed to write CSV file: {e}")
            raise


# Example usage
if __name__ == "__main__":
    converter = GNCDailyClientReportConverterGet()
    converter.convert("inventory.json", "output")
