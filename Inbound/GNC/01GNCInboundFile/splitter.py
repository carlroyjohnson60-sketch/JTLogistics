import os

def split_by_order(input_file, output_dir):
    """
    Splits a big file into multiple files grouped by order number.
    Order number located at characters 15-25 (1-based) -> python slice [14:25].
    Consecutive lines with same order number go to same file.
    Ignores blank lines. Stops at '#EOT'.
    Skips the first line if it is blank or starts with '#'.
    Returns list of file paths created.
    """
    os.makedirs(output_dir, exist_ok=True)
    created_files = []

    current_order = None
    buffer = []

    with open(input_file, "r", encoding="utf-8") as f:
        for i, raw in enumerate(f):
            line = raw.rstrip("\n")

            # Skip first line if blank OR starts with '#'
            if i == 0 and (not line.strip() or line.strip().startswith("#")):
                continue

            # Skip any blank lines
            if not line.strip():
                continue

            # Stop processing at EOT
            if line.lstrip().startswith("#EOT"):
                break

            order_number = line[14:25].strip()  # 1-based 15-25 -> [14:25]

            if current_order is None:
                current_order = order_number
                buffer.append(line)
            elif order_number == current_order:
                buffer.append(line)
            else:
                out_path = os.path.join(output_dir, f"{current_order}.txt")
                with open(out_path, "w", encoding="utf-8") as out:
                    out.write("\n".join(buffer) + "\n")
                created_files.append(out_path)

                current_order = order_number
                buffer = [line]

        # flush last group
        if buffer:
            out_path = os.path.join(output_dir, f"{current_order}.txt")
            with open(out_path, "w", encoding="latin-1") as out:
                out.write("\n".join(buffer) + "\n")
            created_files.append(out_path)

    return created_files
