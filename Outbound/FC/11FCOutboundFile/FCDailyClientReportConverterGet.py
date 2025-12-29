import os
import json
from collections import defaultdict


class FCDailyClientReportConverterGet:
    """
    Converts FC Adjustments JSON into CSV inventory format.

    CSV Columns:
    Material,Available,Committed,On-Hold,Seconds,Waste,Staged,Total
    """

    def __init__(self):
        self.headers = [
            "Material",
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

        adjustments = data.get("adjustments", [])

        # Aggregate packaged_amount per material
        material_totals = defaultdict(int)

        for adj in adjustments:
            material = adj.get("material", "")
            qty = adj.get("packaged_amount", 0) or 0
            material_totals[material] += qty

        if not material_totals:
            print("[WARNING] No adjustment records found.")
            return None

        rows = []

        for material, available_qty in material_totals.items():
            row = [
                material,          # Material
                available_qty,     # Available
                0,                 # Committed
                0,                 # On-Hold
                0,                 # Seconds
                0,                 # Waste
                0,                 # Staged (always 0)
                available_qty      # Total (Total on Hand)
            ]
            rows.append(row)

        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "inventory_adjustments.csv")

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                # Write header
                f.write(",".join(self.headers) + "\n")

                # Write rows
                for r in rows:
                    f.write(",".join(str(x) for x in r) + "\n")

            print(f"[SUCCESS] Conversion complete -- {len(rows)} records written to {output_path}")
            return output_path

        except Exception as e:
            print(f"[ERROR] Failed to write CSV file: {e}")
            raise


# Example usage
if __name__ == "__main__":
    converter = FCDailyClientReportConverterGet()
    converter.convert("adjustments.json", "output")
