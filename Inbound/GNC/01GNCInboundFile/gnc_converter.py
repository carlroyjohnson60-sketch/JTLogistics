import json
import os
import re
from datetime import datetime
import logging
import requests

logger = logging.getLogger(__name__)


class GNCOrders_Converter:
    FIELD_MAP = [
        ("RECID", 1, 9),
        ("FILL01", 10, 11),
        ("CLTABR", 12, 14),
        ("VC_HOST_CSTNUM", 15, 25),
        ("FILL02", 26, 28),
        ("STADRNAM", 29, 58),
        ("FILL03", 59, 178),
        ("SHPNMP", 179, 228),
        ("SHPNMS", 229, 238),
        ("FILL04", 239, 338),
        ("STADRLN1", 339, 368),
        ("FILL05", 369, 378),
        ("FILL05A", 379, 402),
        ("STADRLN2", 403, 432),
        ("FILL06", 433, 466),
        ("STADRLN3", 467, 496),
        ("FILL07", 497, 530),
        ("STADRCTY", 531, 557),
        ("FILL08", 558, 584),
        ("STADRSTC", 585, 586),
        ("STADRPSZ", 587, 596),
        ("STCTRY_NAME", 597, 598),
        ("FILL09", 599, 602),
        ("STPHNNUM", 603, 612),
        ("BTADRNAM", 613, 642),
        ("FILL10", 643, 922),
        ("BTADRLN1", 923, 952),
        ("FILL11", 953, 986),
        ("BTADRLN2", 987, 1016),
        ("FILL12", 1017, 1050),
        ("BTADRLN3", 1051, 1080),
        ("FILL13", 1081, 1114),
        ("BTADRCTY", 1115, 1141),
        ("FILL14", 1142, 1168),
        ("BTADRSTC", 1169, 1170),
        ("BTADRPSZ", 1171, 1180),
        ("BTCTRY_NAME", 1181, 1182),
        ("FILL15", 1183, 1186),
        ("BTPHNNUM", 1187, 1196),
        ("FILL16", 1197, 1248),
        ("VC_HOST_ORDNUM", 1249, 1261),
        ("FILL17", 1262, 1264),
        ("PRTNUM", 1265, 1276),
        ("FILL18", 1277, 1278),
        ("VC_PRTSTYCOD", 1279, 1280),
        ("FILL19", 1281, 1312),
        ("ORDQTY", 1313, 1317),
        ("FILL20", 1318, 1344),
        ("CPONUM", 1345, 1361),
        ("FILL20A", 1362, 1456),
        ("UC_NOPFLG", 1457, 1458),
        ("UC_SPIFLG", 1459, 1459),
        ("VC_DLVINS", 1460, 1516),
        ("FILL20B", 1517, 1520),
        ("VC_EXTERNAL_ORDNUM", 1521, 1537),
        ("FILL21", 1538, 1686),
        ("SHPMTD", 1687, 1687),
        ("FILL21A", 1688, 1694),
        ("BKPRTNUM", 1695, 1706),
        ("FILL22", 1707, 1742),
        ("BKOQTY", 1743, 1747),
        ("SHPLIN", 1748, 1749),
        ("FILL23", 1750, 1752),
        ("VC_HOST_ORDLIN", 1753, 1756),
        ("FILL24", 1757, 1815),
        ("DELINS", 1816, 2055),
    ]

    @staticmethod
    def _slice(line: str, start: int, end: int) -> str:
        """Extract substring safely from fixed-width line."""
        return (line[start - 1:end] if len(line) >= end else line[start - 1:]).strip()

    @classmethod
    def parse_line(cls, line: str):
        """Parse a single line into a dict using FIELD_MAP."""
        return {name: cls._slice(line, start, end) for name, start, end in cls.FIELD_MAP}

    @staticmethod
    def _extract_int(v, default=0):
        """Extract integer from any string, removing non-digits."""
        if v is None or str(v).strip() == "":
            return default
        s = re.sub(r"\D", "", str(v))
        try:
            return int(s) if s else default
        except Exception:
            return default

    @staticmethod
    def _to_int(v, default=0):
        return GNCOrders_Converter._extract_int(v, default)

    @classmethod
    def convert(cls, dat_path: str, output_dir: str) -> str:
        """Convert a .dat file into a JSON order payload; packaging resolved via material API (owner/project GNC)."""
        # Read and clean lines
        with open(dat_path, "r", encoding="utf-8") as f:
            lines = [ln.rstrip("\n") for ln in f if ln.strip() and ln.strip() != "#EOT"]

        if not lines:
            raise ValueError("No valid records in file")

        order_lines = []
        first_rec = None

        # per-conversion cache to avoid repeated API calls for same material
        material_packaging_cache = {}

        # material API defaults (no cfg available here; uses default URL)
        default_api_url = "https://jtl-footprint-api.wavelength.host/api/materials/get"
        api_url = default_api_url
        api_headers = {}

        def resolve_packaging_via_api(material_code: str) -> str:
            """Call material API and select packaging for the given material_code (lookup)."""
            if not material_code:
                return "EA"
            key = material_code.strip()
            if not key:
                return "EA"
            if key in material_packaging_cache:
                return material_packaging_cache[key]

            packaging = "EA"
            payload = {
                "filters": {
                    "owner": ["GNC"],
                    "project": ["GNC"],
                    "lookup": [key]
                }
            }

            try:
                resp = requests.post(api_url, json=payload, headers=api_headers, timeout=30)
                if resp.ok and resp.content:
                    try:
                        data = resp.json()
                    except Exception:
                        data = {}
                    materials = data.get("materials") if isinstance(data, dict) else None
                    if materials and isinstance(materials, list) and len(materials) > 0:
                        mat = materials[0]
                        packagings = mat.get("packagings") or []
                        if packagings:
                            if len(packagings) == 1:
                                pkg = packagings[0].get("packaging")
                                if pkg:
                                    packaging = str(pkg).strip()
                            else:
                                def base_qty(p):
                                    try:
                                        v = p.get("base_packaging_quantity")
                                        if v is None:
                                            return float("inf")
                                        return int(v)
                                    except Exception:
                                        return float("inf")
                                chosen = min(packagings, key=base_qty)
                                pkg = chosen.get("packaging")
                                if pkg:
                                    packaging = str(pkg).strip()
                else:
                    logger.debug("Material API returned status %s for material %s", getattr(resp, "status_code", None), key)
            except Exception as e:
                logger.warning("Material API lookup failed for %s: %s", key, e)

            material_packaging_cache[key] = packaging or "EA"
            logger.debug("Packaging for %s => %s", key, packaging)
            return packaging or "EA"

        for idx, ln in enumerate(lines, start=1):
            rec = cls.parse_line(ln)
            if first_rec is None:
                first_rec = rec  # save first record for header-level info

            order_id_num = cls._to_int(rec.get("VC_HOST_CSTNUM", ""))

            # Build each order line (material + quantity)
            material = rec.get("PRTNUM", "").strip()
            packaging = resolve_packaging_via_api(material)

            order_line = {
                "line_number": idx,
                "material": material,
                "vendor_lot": rec.get("BKPRTNUM", ""),
                "lot": "",
                "packaging": packaging,
                "packaged_amount": cls._to_int(rec.get("ORDQTY", "0")),
                "upc": "",
                "child_lines": [],
                "custom_fields": [],
                "order_id": order_id_num,
                "cost": 0,
                "price": 0,
            }

            order_lines.append(order_line)

        # Use first record for order-level info
        rec = first_rec
        order_id_num = cls._to_int(rec.get("VC_HOST_CSTNUM", ""))

        # Split name fields
        stadrnam = rec.get("STADRNAM", "").strip()
        if " " in stadrnam:
            first_name, last_name = stadrnam.split(" ", 1)
        else:
            first_name, last_name = stadrnam, ""

        # Shipping address
        ship_to = {
            "type": "Shipping",
            "name": rec.get("STADRNAM", ""),
            "reference": "",
            "attention_of": "",
            "line_1": rec.get("STADRLN1", ""),
            "line_2": rec.get("STADRLN2", ""),
            "city": rec.get("STADRCTY", ""),
            "state": rec.get("STADRSTC", ""),
            "postal_code": rec.get("STADRPSZ", ""),
            "country": rec.get("STCTRY_NAME", ""),
            "lookup": "",
            "phone": rec.get("STPHNNUM", ""),
            "email": "",
            "fax": "",
            "title": "",
            "greeting": "",
            "first_name": first_name,
            "middle_name": "",
            "last_name": last_name,
            "is_residential": False,
            "notes": "",
            "order_id": order_id_num,
        }

        # Billing address
        bill_to = {
            "type": "Billing",
            "name": rec.get("BTADRNAM", ""),
            "reference": "",
            "attention_of": "",
            "line_1": rec.get("BTADRLN1", ""),
            "line_2": rec.get("BTADRLN2", ""),
            "city": rec.get("BTADRCTY", ""),
            "state": rec.get("BTADRSTC", ""),
            "postal_code": rec.get("BTADRPSZ", ""),
            "country": rec.get("BTCTRY_NAME", ""),
            "lookup": "",
            "phone": rec.get("BTPHNNUM", ""),
            "email": "",
            "fax": "",
            "title": "",
            "greeting": "",
            "first_name": first_name,
            "middle_name": "",
            "last_name": last_name,
            "is_residential": False,
            "notes": "",
            "order_id": order_id_num,
        }

        # Convert shipping method
        ship_method_raw = rec.get("SHPMTD", "").strip().upper()
        ship_method_map = {
            "C": "SO",
            "O": "PO",
            "P": "2ND",
            "Q": "GRND",
        }
        ship_method = ship_method_map.get(ship_method_raw, ship_method_raw)

        # Build final JSON
        order_obj = {
            "owner": "GNC",
            "project": "GNC",
            "order_class": "Ecom",
            "owner_reference": rec.get("VC_HOST_ORDNUM", ""),
            "vendor_reference": rec.get("CPONUM", ""),
            "requested_delivery_date": "",
            "warehouse": "2301",
            "carrier": ship_method,
            "carrier_service": ship_method,
            "addresses": [ship_to, bill_to],
            "order_lines": order_lines,
            "custom_fields": [],
            "shipments": [],
            "lookup": rec.get("VC_HOST_ORDNUM", ""),
            "instructions": [],
            "currency": "USD",
            "notes": rec.get("VC_DLVINS", ""),
        }

        final_json = {"orders": [order_obj]}

        # Save JSON file
        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        basename = os.path.splitext(os.path.basename(dat_path))[0]
        json_path = os.path.join(output_dir, f"{basename}_{ts}.json")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(final_json, f, indent=2)

        return json_path
