import os
import json
from datetime import datetime


class FCJsonToDatConverterInventoryAdjustments:
    """
    Converts API JSON 'adjustments' data into Franklin Covey .dat/CSV format (comma-separated, no header).

    Output fields per record:
    TRNDTE,TRNTIME,ORACLE_REF,ITEM_NUMBER,TRANS_QUANTITY,TRANS_UOM,
    SUBINV_CODE,DSP_SEG1,TRANS_TYPE,FILLER

    Notes:
    - TRANS_TYPE: 31 if quantity < 0, else 41
    - TRANS_UOM: always "EA"
    - SUBINV_CODE: always "SHIPPABLE"
    - ORACLE_REF: always 0
    - FILLER: "X,PSG"
    - Skip record if DSP_SEG1 (Reason Code) == "RETURNS"
    """

    # ✅ Reason code mapping table
    REASON_CODE_MAP = {
        "CLT USE": "ASSIST", "CLT SPLE": "ASSIST", "SCB": "ASSIST", "INOUT": "ASSIST", "01": "ASSIST",
        "RETURNS": "RETURNS", "SCRAP": "ASSIST", "ASSIST": "ASSIST", "CYCLE CNT": "CYCLE CNT",
        "DAMAGE": "DAMAGE", "IUSE": "IUSE", "CMP": "CMP", "KIT": "KIT", "RTV": "RTV",
        "SKU CHANGE": "SKU CHANGE", "REWORK": "REWORK", "DESTROY": "DESTROY", "03": "DESTROY",
        "RECYCLE": "RECYCLE", "PICK ERROR": "PICK ERROR", "QTY ERROR": "QTY ERROR",
        "UOM": "UOM", "RTN MFR": "RTN MFR", "PI": "PI", "HPE MISC": "HPE MISC",
        "HPE DAMAGE": "HPE DAMAGE", "RECEIPT DAMAGE": "RECEIPT DAMAGE", "MISC": "MISC"
    }

    def __init__(self):
        self.headers = [
            "TRNDTE", "TRNTIME", "ORACLE_REF", "ITEM_NUMBER",
            "TRANS_QUANTITY", "TRANS_UOM", "SUBINV_CODE",
            "DSP_SEG1", "TRANS_TYPE", "FILLER"
        ]

    def convert(self, json_path, output_dir):
        # Load and validate JSON
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Handle if the file is a JSON string
        if isinstance(data, str):
            data = json.loads(data)

        adjustments = data.get("adjustments", [])
        if not isinstance(adjustments, list):
            raise ValueError(f"Expected 'adjustments' to be a list, got {type(adjustments).__name__}")

        rows = []

        for adj in adjustments:
            # Defensive: skip invalid objects
            if not isinstance(adj, dict):
                continue

            completed_on = adj.get("completed_on", "")
            trndte, trntime = "", ""

            # Parse timestamp into date/time
            if completed_on:
                try:
                    dt = datetime.fromisoformat(completed_on.replace("Z", "+00:00"))
                    trndte = dt.strftime("%Y%m%d")
                    trntime = dt.strftime("%H%M%S")
                except Exception:
                    pass

            qty = adj.get("packaged_amount", 0)
            raw_reason = adj.get("project", "").strip().upper()

            # ✅ Skip if reason = RETURNS
            if raw_reason == "RETURNS":
                continue

            dsp_seg1 = self.REASON_CODE_MAP.get(raw_reason, raw_reason or "MISC")

            row = [
                trndte,                  # TRNDTE
                trntime,                 # TRNTIME
                0,                       # ORACLE_REF
                adj.get("material", ""), # ITEM_NUMBER
                qty,                     # TRANS_QUANTITY
                "EA",                    # TRANS_UOM
                "SHIPPABLE",             # SUBINV_CODE
                dsp_seg1,                # DSP_SEG1
                31 if qty < 0 else 41,   # TRANS_TYPE
                "X,PSG"                  # FILLER
            ]
            rows.append(row)

        if not rows:
            print("[WARNING] No valid adjustment records found.")
            return None

        # Ensure output dir
        os.makedirs(output_dir, exist_ok=True)

        # Use generic output filename - processor will rename with proper {datetime}
        output_path = os.path.join(output_dir, "output.dat")

        try:
            # Write CSV (.dat) without header
            with open(output_path, "w", encoding="utf-8") as f:
                for r in rows:
                    f.write(",".join(str(x) for x in r) + "\n")

            print(f"[SUCCESS] Conversion complete -- {len(rows)} records written to {output_path}")
            return output_path
        except Exception as e:
            print(f"[ERROR] Error writing output file {output_path}: {e}")
            raise


# Example usage
if __name__ == "__main__":
    converter = FCJsonToDatConverterInventoryAdjustments()
    converter.convert("api_response_05FCOutboundFile.json", "output")
