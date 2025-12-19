"""Base converter class that handles JSON to fixed width or CSV conversion.

Usage example:
    converter = FixedWidthConverter()  # or CSVConverter
    output_path = converter.convert(json_path, output_dir)
"""
import os
import json
import csv
from datetime import datetime
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class BaseConverter(ABC):
    """Abstract base class for all converters"""
    
    @abstractmethod
    def convert(self, json_path: str, output_dir: str) -> str:
        """Convert JSON to output format and return path to created file"""
        pass
    
    def _load_json(self, json_path: str) -> Dict[str, Any]:
        """Load and validate JSON data"""
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        orders = data.get("orders", [])
        if not orders:
            paging = data.get("paging", {})
            total_records = paging.get("total_records", 0)
            returned_records = paging.get("returned_records", 0)
            
            if total_records == 0 and returned_records == 0:
                raise ValueError("No records found (total_records and returned_records are 0)")
        return data


class FixedWidthConverter(BaseConverter):
    """Converts JSON to fixed width format using field specifications"""

    def __init__(self, field_specs: List[tuple], header: Optional[str] = None, trailer_format: Optional[str] = None):
        """
        Initialize with field specifications and optional header/trailer
        
        Args:
            field_specs: List of tuples (name, length, format_type, default)
            header: Optional header record format
            trailer_format: Optional trailer record format (will get record count)
        """
        self.field_specs = field_specs
        self.header = header
        self.trailer_format = trailer_format

    @staticmethod
    def to_julian(dt: datetime, three_digit_year: bool = False) -> str:
        """Convert datetime → YYJJJ or YYYJJJ format"""
        year = dt.year if three_digit_year else dt.year % 100
        day_of_year = dt.timetuple().tm_yday
        return f"{year:02d}{day_of_year:03d}" if not three_digit_year else f"{year:03d}{day_of_year:03d}"

    def format_field(self, value: Any, length: int, ftype: str) -> str:
        """Format a field value according to specifications"""
        if ftype == "left":
            return str(value).ljust(length)[:length]
        elif ftype == "right":
            return str(value).rjust(length, "0")[:length]
        elif ftype == "zero":
            return str(value).rjust(length, "0")[:length]
        elif ftype == "julian":
            return value
        elif ftype == "time":
            return value
        else:
            return str(value).ljust(length)[:length]

    @abstractmethod
    def map_field(self, field_name: str, order: Dict[str, Any], line: Dict[str, Any]) -> Any:
        """Map JSON data to field value - must be implemented by subclasses"""
        pass

    def build_record(self, order: Dict[str, Any], line: Dict[str, Any]) -> str:
        """Build a single record from order and line data"""
        record = []
        for name, length, ftype, default in self.field_specs:
            value = self.map_field(name, order, line) or default
            record.append(self.format_field(value, length, ftype))
        return "".join(record)

    def convert(self, json_path: str, output_dir: str) -> str:
        """Convert JSON to fixed width format and return output path"""
        data = self._load_json(json_path)
        orders = data.get("orders", [])

        # Generate output records
        detail_lines = []
        for order in orders:
            for line in order.get("order_lines", []):
                detail_lines.append(self.build_record(order, line))

        # Create output path
        os.makedirs(output_dir, exist_ok=True)
        base_name = os.path.splitext(os.path.basename(json_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.dat")

        # Write output file
        with open(output_path, "w", encoding="utf-8") as f:
            if self.header:
                f.write(self.header + "\n")
            f.write("\n".join(detail_lines))
            if self.trailer_format:
                trailer = self.trailer_format.format(record_count=len(detail_lines))
                f.write("\n" + trailer)

        return output_path


class CSVConverter(BaseConverter):
    """Converts JSON to CSV format using field mappings"""

    def __init__(self, headers: List[str]):
        """
        Initialize with CSV headers
        
        Args:
            headers: List of column headers
        """
        self.headers = headers

    @abstractmethod
    def map_row(self, order: Dict[str, Any], line: Dict[str, Any]) -> Dict[str, Any]:
        """Map JSON data to CSV row - must be implemented by subclasses"""
        pass

    def convert(self, json_path: str, output_dir: str) -> str:
        """Convert JSON to CSV format and return output path"""
        data = self._load_json(json_path)
        orders = data.get("orders", [])

        # Generate rows
        rows = []
        for order in orders:
            for line in order.get("order_lines", []):
                rows.append(self.map_row(order, line))

        # Create output path
        os.makedirs(output_dir, exist_ok=True)
        base_name = os.path.splitext(os.path.basename(json_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.csv")

        # Write CSV file
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writeheader()
            writer.writerows(rows)

        return output_path