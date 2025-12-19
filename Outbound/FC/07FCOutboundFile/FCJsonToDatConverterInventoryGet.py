import os
import json
from datetime import datetime


class FCJsonToDatConverterMaterialPackaging:
    """
    Converts FC JSON materials into .dat file (comma-separated, no header).

    Output format per line:
    SKU,Unit Weight,Unit Volume,Unit Height,Unit Width,Unit Length,Units Per Case

    Example:
    TOMTESTCS3,42.0,13392,9,31,48,1
    """

    def __init__(self):
        self.headers = [
            "SKU",
            "Unit Weight",
            "Unit Volume",
            "Unit Height",
            "Unit Width",
            "Unit Length",
            "Units Per Case"
        ]

    def convert(self, json_path, output_dir):
        # Load JSON
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        materials = data.get("materials", [])
        rows = []

        for material in materials:
            sku = material.get("lookup", "")
            packagings = material.get("packagings", [])

            # Only pick packagings that are ready to ship
            for pkg in packagings:
                """if not pkg.get("is_ready_to_ship", False):
                    continue"""

                # Extract values into variables
                gross_weight = pkg.get("gross_weight", 0) or 0
                gross_volume = pkg.get("gross_volume", 0) or 0
                height = pkg.get("height", 0) or 0
                width = pkg.get("width", 0) or 0
                length = pkg.get("length", 0) or 0
                units_per_case = pkg.get("sub_packaging_quantity") or pkg.get("base_packaging_quantity") or 1

                # Compute unit weight
                try:
                    unit_weight = round(float(gross_weight) / float(units_per_case), 3)
                except ZeroDivisionError:
                    unit_weight = 0

                row = [
                    sku,
                    unit_weight,
                    round(float(gross_volume), 3),
                    round(float(height), 2),
                    round(float(width), 2),
                    round(float(length), 2),
                    int(units_per_case)
                ]
                rows.append(row)

        # No valid packagings found
        if not rows:
            print("[WARNING] No 'ready to ship' packagings found.")
            return None

        os.makedirs(output_dir, exist_ok=True)
        # Use generic output filename - processor will rename with proper {datetime}
        output_path = os.path.join(output_dir, "output.dat")

        try:
            # Write .dat file (no header)
            with open(output_path, "w", encoding="utf-8") as f:
                for r in rows:
                    f.write(",".join(str(x) for x in r) + "\n")

            print(f"[SUCCESS] Conversion complete -- {len(rows)} records written to {output_path}")
            return output_path
        except Exception as e:
            print(f"[ERROR] Error writing output file {output_path}: {e}")
            raise


# Example usage:
if __name__ == "__main__":
    converter = FCJsonToDatConverterMaterialPackaging()
    converter.convert("materials.json", "output")
