import csv
import json
import os
from datetime import datetime


class inventoryGetConverter:
    """
    Converts API JSON response from inventory_get_api into formatted CSV.

    This converter is robust to the API response file containing an empty
    body or a plain string. It will attempt to parse JSON from the file
    and gracefully return None when no usable 'adjustments' are present.
    """

    def convert(self, json_path, output_csv_path):
        # Read file contents first
        if not os.path.exists(json_path):
            print(f"[WARNING] Response file not found: {json_path}")
            return None

        with open(json_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()

        if not content:
            # Empty response is acceptable; nothing to convert
            print(f"[INFO] Empty API response in {json_path}; no records to convert.")
            return None

        # Try to parse JSON. The response file may contain raw JSON text
        # or a JSON-encoded string. Try common strategies.
        data = None
        try:
            # Primary: parse as JSON object/array
            data = json.loads(content)
        except json.JSONDecodeError:
            # Secondary: sometimes the file contains a quoted JSON string
            try:
                # e.g. content = '"{...}"' or '"[...]"'
                unquoted = json.loads(content)
                if isinstance(unquoted, str):
                    data = json.loads(unquoted)
                else:
                    data = unquoted
            except Exception:
                print(f"[ERROR] Unable to parse JSON from response file: {json_path}")
                return None

        if not isinstance(data, dict):
            # If the top-level is a list or other, normalize into dict
            if isinstance(data, list):
                data = {"adjustments": data}
            else:
                print(f"[INFO] Parsed response is not an object; no 'adjustments' found.")
                return None

        adjustments = data.get("adjustments", [])
        if not adjustments:
            print(f"[INFO] No adjustments found in response {json_path}; nothing to write.")
            return None

        rows = []

        for adj in adjustments:
            # Parse completed_on → YYYYMMDDHHMISS
            completed_on = adj.get("completed_on", "")
            trndte = ""
            if completed_on:
                try:
                    dt = datetime.fromisoformat(completed_on.replace("Z", "+00:00"))
                    trndte = dt.strftime("%Y%m%d%H%M%S")
                except Exception:
                    trndte = ""

            row = {
                "trndte": trndte,
                "client_id": "MRS",
                "prtnum": adj.get("material", ""),
                "trnqty": adj.get("packaged_amount", 0),
                "stkuom": "EA",
                "reacod": "",        # Reason Code
                "uc_adj_comm": "",   # Comment
                "invsts": "A"        # Status
            }
            rows.append(row)

        # Define field order
        fieldnames = [
            "trndte", "client_id", "prtnum", "trnqty",
            "stkuom", "reacod", "uc_adj_comm", "invsts"
        ]

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_csv_path) or '.', exist_ok=True)

        # Write CSV file
        with open(output_csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(rows)

        print(f"✅ CSV successfully created: {output_csv_path}")
        print(f"📄 {len(rows)} record(s) written.")
        return output_csv_path
