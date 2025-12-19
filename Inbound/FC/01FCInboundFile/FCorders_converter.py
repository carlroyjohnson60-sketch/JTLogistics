# orders_converter.py
import json
import os
import re
from datetime import datetime
import logging
import requests

import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

# NOTE: MaterialPackagingResolver is no longer used here.
logger = logging.getLogger(__name__)


class FCOrdersConverter:
    HEADER_MAP = [
        ("RECID", 1, 2),
        ("CLIENT_ID", 3, 12),
        ("ORDNUM", 13, 32),
        ("CPONUM", 33, 52),
        ("CPODTE", 53, 60),
        ("VC_SHIPID", 61, 80),
        ("SHIP_METHOD", 81, 90),
        ("BTATTN", 91, 150),
        ("BTNAME", 151, 190),
        ("BTCOMP", 191, 250),
        ("BTADR1", 251, 290),
        ("BTADR2", 291, 330),
        ("BTADR3", 331, 370),
        ("BTCITY", 371, 400),
        ("BTSTATE", 401, 440),
        ("BTZIP", 441, 460),
        ("BTCTRY", 461, 490),
        ("BTADRTYP", 491, 494),
        ("BTPHONE", 495, 514),
        ("BTEMAIL", 515, 594),
        ("STATTN", 595, 654),
        ("STNAME", 655, 694),
        ("STCOMP", 695, 754),
        ("STADR1", 755, 794),
        ("STADR2", 795, 834),
        ("STADR3", 835, 874),
        ("STCITY", 875, 904),
        ("STSTATE", 905, 944),
        ("STZIP", 945, 964),
        ("STCTRY", 965, 994),
        ("STADRTYP", 995, 998),
        ("STPHONE", 999, 1018),
        ("STEMAIL", 1019, 1098),
        ("DISTNUM", 1099, 1118),
        ("MKTCOD", 1119, 1128),
        ("DLVINS", 1129, 1368),
        ("DUETOT", 1369, 1381),
        ("TAXSUB", 1382, 1394),
        ("ORDTYP", 1395, 1406),
        ("TAX", 1407, 1419),
        ("AMTPAID", 1420, 1432),
        ("SHPTOTAL", 1433, 1445),
        ("ORDDSCAMT", 1446, 1458),
        ("CMNT", 1459, 1708),
        ("ASSTLBL", 1709, 1709),
        ("PMTMTD", 1710, 1769),
        ("VC_PCKINSTR", 1770, 1809),
        ("VC_EXTERNAL_ORDNUM", 1810, 1829),
    ]

    ORDERLINE_MAP = [
        ("RECID", 1, 2),
        ("ORDNUM", 3, 22),
        ("VC_SHIPID", 23, 42),
        ("ORDLIN", 43, 46),
        ("SUBLINE", 47, 50),
        ("PRTNUM", 51, 80),
        ("ORDQTY", 81, 90),
        ("EDLVDTE", 91, 98),
        ("LDLVDTE", 99, 106),
        ("ESHPDTE", 107, 114),
        ("LSHPDTE", 115, 122),
        ("VC_CTNNUM", 123, 154),
        ("VC_BCKORDFLG", 155, 156),
        ("VC_COMMENT", 157, 406),
        ("VC_PCKINSTR", 407, 661),
        ("VC_PCKSTS", 662, 673),
        ("VC_SHPAMT", 674, 686),
        ("VC_TAXAMT", 687, 699),
        ("VC_UNTDSC", 700, 712),
        ("VC_UNTWGT", 713, 725),
        ("VC_UNTPRC", 726, 738),
        ("VC_WMSSTS", 739, 750),
    ]

    @staticmethod
    def _slice(line: str, start: int, end: int) -> str:
        return (line[start - 1:end] if len(line) >= end else line[start - 1:]).strip()

    @classmethod
    def parse_line(cls, line: str, field_map):
        return {name: cls._slice(line, start, end) for name, start, end in field_map}

    @staticmethod
    def _parse_date_field(value: str):
        v = (value or "").strip()
        if not v:
            return None
        if v.isdigit() and len(v) == 8:
            try:
                return datetime.strptime(v, "%Y%m%d").isoformat() + "Z"
            except Exception:
                return v
        return v

    @staticmethod
    def _extract_int(v, default=0):
        """Extract digits from string and return as int; fallback to default."""
        if v is None:
            return default
        s = str(v).strip()
        if not s:
            return default
        s_digits = re.sub(r"\D", "", s)
        if not s_digits:
            return default
        try:
            return int(s_digits)
        except Exception:
            return default

    @staticmethod
    def _to_int(v, default=0):
        """Generic integer coercion, forgiving (removes non-digits)."""
        return FCOrdersConverter._extract_int(v, default)

    @staticmethod
    def _to_float(v, default=0.0):
        if v is None:
            return default
        s = str(v).strip().replace(",", "")
        if not s:
            return default
        s_clean = re.sub(r"[^\d\.\-]", "", s)
        try:
            return float(s_clean)
        except Exception:
            return default

    @classmethod
    def convert(cls, dat_path: str, output_dir: str, *, cfg=None, oauth=None) -> str:
        """
        Convert dat file to JSON for FC orders.
        Packaging resolution is done here by calling the material API directly.
        """
        with open(dat_path, "r", encoding="utf-8") as f:
            lines = [ln.rstrip("\n") for ln in f]

        if not lines:
            raise ValueError("No data in file")

        header = cls.parse_line(lines[0], cls.HEADER_MAP)
        order_id_num = cls._extract_int(header.get("ORDNUM", ""), default=0)

        # small per-conversion cache to avoid repeated API calls for same material
        material_packaging_cache = {}

        btadrnam = header.get("BTNAME", "").strip()
        if " " in btadrnam:
            bfirst_name, blast_name = btadrnam.split(" ", 1)
        else:
            bfirst_name, blast_name = btadrnam, ""

        stadrnam = header.get("STNAME", "").strip()
        if " " in stadrnam:
            sfirst_name, slast_name = stadrnam.split(" ", 1)
        else:
            sfirst_name, slast_name = stadrnam, ""
        # --- Ship method conversion ---
        ship_method_raw = header.get("SHIP_METHOD", "").strip().upper()
        ship_method_map = {
            "BWAY": "GROUND",
            "IOP": "INTLP",
            "UPS2": "2DAY",
            "UPS3": "3DAY",
            "UPSG": "GRNDC",
            "NDAYAM": "NDAYAM",
            "UPS4": "NDAYSAV",
            "MAIL": "STD",
            "FHO": "HOME",
            "F18": "SP",
            "F01": "GRND",
            "F02": "GRNDH",
            "F03": "PO",
            "F04": "SO",
            "F05": "2ND",
            "F06": "2AM",
            "F07": "FO",
            "F12": "3FR",
            "01": "FEDG",
        }
        ship_method = ship_method_map.get(ship_method_raw, ship_method_raw)
        order_lines = []

        # material API defaults
        default_api_url = "https://jtl-footprint-api.wavelength.host/api/materials/get"
        api_cfg = (cfg.data.get("material_api") if cfg and getattr(cfg, "data", None) else {}) or {}
        api_url_cfg = api_cfg.get("url") or default_api_url
        api_headers_cfg = api_cfg.get("headers") or {}

        def resolve_packaging_via_api(material_code: str) -> str:
            if not material_code:
                return "1"
            key = material_code.strip()
            if not key:
                return "2"
            if key in material_packaging_cache:
                return material_packaging_cache[key]
            packaging = "3"
            payload = {
                "filters": {
                    "owner": ["FC"],
                    "project": ["FC"],
                    "lookup": [key]
                }
            }
            headers = {}
            # prefer oauth headers if available
            if oauth:
                try:
                    headers = oauth.get_auth_headers() or {}
                except Exception:
                    headers = {}
            # overlay any configured headers
            try:
                if isinstance(api_headers_cfg, dict):
                    headers.update(api_headers_cfg)
            except Exception:
                pass

            try:
                resp = requests.post(api_url_cfg, json=payload, headers=headers, timeout=int(api_cfg.get("timeout", 30)))
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
                                # choose packaging with smallest numeric base_packaging_quantity
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
            except Exception as e:
                logger.warning("Material API lookup failed for %s: %s", key, e)

            material_packaging_cache[key] = packaging or "EA"
            logger.debug("Packaging for %s => %s", key, packaging)
            return packaging or "EA"

        for idx, ln in enumerate(lines[1:], start=1):
            rec = cls.parse_line(ln, cls.ORDERLINE_MAP)
            line_number = cls._to_int(rec.get("ORDLIN") or idx, default=idx)

            material = rec.get("PRTNUM", "").strip()
            # resolve packaging via API (always)
            packaging = resolve_packaging_via_api(material)

            packaged_amount = cls._to_int(rec.get("ORDQTY", "0"))
            price_original = cls._to_float(rec.get("VC_UNTPRC", "0"))
            price = round(price_original * 0.0001, 4)

            order_line_obj = {
                "line_number": line_number,
                "material": rec.get("PRTNUM", ""),
                "vendor_lot": rec.get("VC_CTNNUM", "") or "",
                "lot": "",
                "packaging": packaging,
                "packaged_amount": packaged_amount,
                "upc": "",
                "child_lines": [],
                "custom_fields": [],
                "order_id": order_id_num,
                "cost": 0,
                "price": price,
                "delivery_dates": {
                    "earliest": cls._parse_date_field(rec.get("EDLVDTE", "")),
                    "latest": cls._parse_date_field(rec.get("LDLVDTE", "")),
                },
                "ship_dates": {
                    "earliest": cls._parse_date_field(rec.get("ESHPDTE", "")),
                    "latest": cls._parse_date_field(rec.get("LSHPDTE", "")),
                },
                "backorder_flag": rec.get("VC_BCKORDFLG", ""),
                "comment": rec.get("VC_COMMENT", ""),
                "status": rec.get("VC_PCKSTS", ""),
                "vendor_status": rec.get("VC_WMSSTS", ""),
            }
            order_lines.append(order_line_obj)

        # --- Address objects ---
        bill_to = {
            "type": "Billing",
            "name": header.get("BTNAME", ""),
            "reference": header.get("BTCOMP", "")[:32],
            "attention_of": header.get("BTATTN", ""),
            "line_1": header.get("BTADR1", ""),
            "line_2": header.get("BTADR2", ""),
            "city": header.get("BTCITY", ""),
            "state": header.get("BTSTATE", ""),
            "postal_code": header.get("BTZIP", ""),
            "country": header.get("BTCTRY", ""),
            "lookup": "",
            "phone": header.get("BTPHONE", ""),
            "email": header.get("BTEMAIL", ""),
            "fax": "",
            "title": "",
            "greeting": "",
            "first_name": bfirst_name,
            "middle_name": "",
            "last_name": blast_name[:32],
            "is_residential": False,
            "notes": header.get("CMNT", ""),
            "order_id": order_id_num,
        }

        ship_to = {
            "type": "Shipping",
            "name": header.get("STNAME", ""),
            "reference": header.get("STCOMP", "")[:32],
            "attention_of": header.get("STATTN", ""),
            "line_1": header.get("STADR1", ""),
            "line_2": header.get("STADR2", ""),
            "city": header.get("STCITY", ""),
            "state": header.get("STSTATE", ""),
            "postal_code": header.get("STZIP", ""),
            "country": header.get("STCTRY", ""),
            "lookup": "",
            "phone": header.get("STPHONE", ""),
            "email": header.get("STEMAIL", ""),
            "fax": "",
            "title": "",
            "greeting": "",
            "first_name": sfirst_name,
            "middle_name": "",
            "last_name": slast_name[:32],
            "is_residential": False,
            "notes": header.get("CMNT", ""),
            "order_id": order_id_num,
        }

        # --- Custom fields ---
        custom_fields = []
        if header.get("CPONUM", "").strip():
            custom_fields.append({
                "name": "CustomerPO",
                "value": header["CPONUM"].strip()
            })

        # --- Build final order object ---
        order_obj = {
            "owner": "FC",
            "project": "FC",
            "order_class": "Ecom",
            "owner_reference": header.get("ORDNUM", ""),
            "vendor_reference": header.get("CPONUM", ""),
            "requested_delivery_date": cls._parse_date_field(header.get("CPODTE", "")),
            "warehouse": "2301",
            "carrier": ship_method,
            "carrier_service": ship_method,
            "addresses": [bill_to, ship_to],
            "order_lines": order_lines,
            "custom_fields": custom_fields,
            "shipments": [],
            "lookup": header.get("ORDNUM", ""),
            "instructions": [],
            "currency": "USD",
            "notes": header.get("DLVINS", ""),
        }

        final_json = {"orders": [order_obj]}

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(output_dir, exist_ok=True)
        json_path = os.path.join(output_dir, f"FCorder_{order_id_num}_{ts}.json")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(final_json, f, indent=2)

        return json_path
