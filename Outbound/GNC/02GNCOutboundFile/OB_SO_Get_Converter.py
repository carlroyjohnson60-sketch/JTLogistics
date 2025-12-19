"""Converter for GNC outbound orders to fixed width format."""
from datetime import datetime
from typing import Dict, Any
from common.converters import FixedWidthConverter


class OrdersFixedLengthConverter(FixedWidthConverter):
    """Converts GNC orders from JSON to fixed width format"""

    # Field specifications
    FIELD_SPECS = [
        ("CLIENTABBR", 3, "left", "MRS"),       # static or from JSON
        ("CDSTRKNUM", 13, "right", ""),         # order.owner_reference
        ("PRODNBR", 12, "left", ""),            # order_lines.material
        ("PRODSIZE", 2, "left", ""),            # blank
        ("PRODSTYLE", 2, "left", ""),           # blank
        ("PRODCOLOR", 2, "left", ""),           # blank
        ("QTYTOSHIP", 3, "zero", 0),            # order_lines.packaged_amount
        ("QTYBACKORD", 3, "zero", 0),           # always 0
        ("HEADERFLAG", 1, "left", "Y"),         # static
        ("ODASHPDT", 5, "julian", None),        # ship date → YYJJJ (Julian)
        ("ODASHPCST", 7, "zero", 0),            # cost default 0
        ("ODASHPMTD", 1, "left", ""),           # order.carrier_service
        ("SHPTRKNUM", 30, "left", ""),          # order.shipments[0].tracking_identifier
        ("CREATEDATE", 5, "julian", None),      # file creation date
        ("CREATETIME", 6, "time", None),        # file creation time
    ]

    def __init__(self):
        """Initialize with field specs and header/trailer formats"""
        header = "000" + " " * 50  # Header format from original
        trailer_format = "999{record_count:013d}" + " " * 37  # Trailer format with record count
        super().__init__(self.FIELD_SPECS, header, trailer_format)

    def map_field(self, field_name: str, order: Dict[str, Any], line: Dict[str, Any]) -> Any:
        """Map JSON fields to fixed width fields using original logic"""
        if field_name == "CLIENTABBR":
            return "MRS"
        elif field_name == "CDSTRKNUM":
            return order.get("owner_reference", "")
        elif field_name == "PRODNBR":
            return line.get("material", "")
        elif field_name == "QTYTOSHIP":
            return line.get("packaged_amount", 0)
        elif field_name == "QTYBACKORD":
            return 0
        elif field_name == "HEADERFLAG":
            return "Y"
        elif field_name == "ODASHPDT":
            return self.to_julian(datetime.now())
        elif field_name == "ODASHPCST":
            return 0
        elif field_name == "ODASHPMTD":
            return order.get("carrier_service", "")
        elif field_name == "SHPTRKNUM":
            shipments = order.get("shipments", [])
            return shipments[0].get("tracking_identifier", "") if shipments else ""
        elif field_name == "CREATEDATE":
            return self.to_julian(datetime.now())
        elif field_name == "CREATETIME":
            return datetime.now().strftime("%H%M%S")
        return None

        all_lines = [header] + detail_lines + [trailer]

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, f"order_{ts}.txt")

        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(all_lines))

        return out_path
