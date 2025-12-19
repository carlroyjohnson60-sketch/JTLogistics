import csv
import json
from datetime import datetime

class OB_JsonToCsvConverter:
    """
    Converts API JSON with nested 'orders', 'addresses', and 'order_lines'
    into a flat CSV with FranklinCovey-style headers.
    """

    HEADERS = [
        "Client", "HP Order", "CDS Order", "Date Entered", "Name",
        "Address Line 1", "Address Line 2", "Address Line 3",
        "City", "State", "Zip", "Phone", "Ship Date",
        "Carrier", "Service Level", "Part Number",
        "Shipped Qty", "Carton Number", "Pro/Tracking Number",
        "Weight", "Status"
    ]

    def convert(self, json_file_path, output_csv_path):
        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        orders = data.get("orders", [])
        rows = []
        # --- Stop if total_records and returned_records are 0 ---
        paging = data.get("paging", {})
        total_records = paging.get("total_records", 0)
        returned_records = paging.get("returned_records", 0)

        if total_records == 0 and returned_records == 0:
            print("⚠️ No records found (total_records and returned_records are 0). Processing stopped.")
            return None
        
        for order in orders:
            # Top-level order info
            client = order.get("owner", "")
            hp_order = order.get("vendor_reference", "")
            cds_order = order.get("lookup", "")
            date_entered = order.get("created_on", "")
            ship_date = order.get("shipped_on", "")
            carrier = order.get("carrier", "")
            service_level = order.get("carrier_service", "")
            status = order.get("status", "")

            # Get addresses (ShipTo preferred, BillTo fallback)
            addresses = order.get("addresses", [])
            ship_to = next((a for a in addresses if a.get("type", "").lower() == "shipto"), None)
            bill_to = next((a for a in addresses if a.get("type", "").lower() == "billto"), None)

            # Fallback logic: use BillTo if ShipTo is missing or empty
            address_used = ship_to or bill_to

            name = address_used.get("name", "") if address_used else ""
            address1 = address_used.get("line_1", "") if address_used else ""
            address2 = address_used.get("line_2", "") if address_used else ""
            address3 = address_used.get("line_3", "") if address_used else ""
            city = address_used.get("city", "") if address_used else ""
            state = address_used.get("state", "") if address_used else ""
            zipcode = address_used.get("postal_code", "") if address_used else ""
            phone = address_used.get("phone", "") if address_used else ""

            # Shipment details
            shipments = order.get("shipments", [])
            tracking_number = shipments[0].get("tracking_identifier", "") if shipments else ""
            carton_number = shipments[0].get("reference_number", "") if shipments else ""
            weight = shipments[0].get("gross_weight", "") if shipments else ""

            # Loop through each order line
            for line in order.get("order_lines", []):
                part_number = line.get("material", "")
                shipped_qty = line.get("packaged_amount", 0)

                row = {
                    "Client": "MRS",
                    "HP Order": hp_order,
                    "CDS Order": cds_order,
                    "Date Entered": date_entered,
                    "Name": name,
                    "Address Line 1": address1,
                    "Address Line 2": address2,
                    "Address Line 3": address3,
                    "City": city,
                    "State": state,
                    "Zip": zipcode,
                    "Phone": phone,
                    "Ship Date": ship_date,
                    "Carrier": carrier,
                    "Service Level": service_level,
                    "Part Number": part_number,
                    "Shipped Qty": shipped_qty,
                    "Carton Number": carton_number,
                    "Pro/Tracking Number": tracking_number,
                    "Weight": weight,
                    "Status": status
                }
                rows.append(row)

        # Write to CSV
        with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.HEADERS)
            writer.writeheader()
            writer.writerows(rows)

        print(f"✅ Conversion complete: {len(rows)} rows written to {output_csv_path}")


if __name__ == "__main__":
    converter = OB_JsonToCsvConverter()
    converter.convert("input.json", "output.csv")