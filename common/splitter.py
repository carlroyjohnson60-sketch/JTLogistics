import os
import logging
from typing import List

class FileSplitter:
    """
    Handles splitting of fixed-width files based on specified field positions.
    Supports various splitting strategies with configurable field positions.
    """
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def split_by_field(self, file_path: str, start: int, end: int, out_dir: str) -> List[str]:
        """
        Splits a file by order number field located at specified start-end positions (1-based).
        Order numbers are extracted from specified character positions.
        Consecutive lines with same order number go to same output file.
        
        Args:
            file_path (str): Path to the input file to split
            start (int): Start position of the field (1-based indexing)
            end (int): End position of the field (1-based indexing)
            out_dir (str): Directory where split files will be saved
            
        Returns:
            List[str]: List of paths to created files
            
        Special handling:
        - Skips first line if it starts with '#'
        - Skips blank lines
        - Stops at '#EOT' marker
        - Groups consecutive lines with same field value
        """
        os.makedirs(out_dir, exist_ok=True)
        created_files = []
        
        current_order = None
        buffer = []

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for i, raw in enumerate(f):
                    line = raw.rstrip("\n")
                    
                    # Skip the first line if it starts with '#'
                    if i == 0 and line.strip().startswith("#"):
                        continue

                    if not line.strip():
                        continue
                    if line.strip() == "#EOT":
                        break

                    # Extract order number from specified position (converting from 1-based to 0-based)
                    order_number = line[start-1:end].strip()

                    if current_order is None:
                        current_order = order_number
                        buffer.append(line)
                    elif order_number == current_order:
                        buffer.append(line)
                    else:
                        out_path = os.path.join(out_dir, f"{current_order}.txt")
                        with open(out_path, "w", encoding="utf-8") as out:
                            out.write("\n".join(buffer) + "\n")
                        created_files.append(out_path)
                        self.logger.debug(f"Created split file: {out_path}")

                        current_order = order_number
                        buffer = [line]

                # Write the last buffer if any
                if buffer:
                    out_path = os.path.join(out_dir, f"{current_order}.txt")
                    with open(out_path, "w", encoding="utf-8") as out:
                        out.write("\n".join(buffer) + "\n")
                    created_files.append(out_path)
                    self.logger.debug(f"Created split file: {out_path}")

            return created_files
            
        except Exception as e:
            self.logger.error(f"Error splitting file {file_path}: {str(e)}")
            raise