"""
File I/O utilities for reading compressed and uncompressed JSONL files.
"""

import gzip
import json
from pathlib import Path
from typing import Any, Dict, Iterator

# JSON handling with fallback
try:
    import orjson

    JSON_LIB = "orjson"
except ImportError:
    JSON_LIB = "json"


def read_jsonl_file(file_path: Path) -> Iterator[Dict[str, Any]]:
    """
    Read a JSONL file (compressed or uncompressed).

    Args:
        file_path: Path to the JSONL file

    Yields:
        Parsed JSON records
    """
    if file_path.suffix == ".gz":
        file_handle = gzip.open(file_path, "rt", encoding="utf-8")
    else:
        file_handle = file_path.open("r", encoding="utf-8")

    try:
        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            if JSON_LIB == "orjson":
                record = orjson.loads(line)
            else:
                record = json.loads(line)
            yield record
    finally:
        file_handle.close()


def write_jsonl_file(
    file_path: Path, records: Iterator[Dict[str, Any]], compress: bool = True
):
    """
    Write records to a JSONL file with optional compression.

    Args:
        file_path: Output file path
        records: Iterator of records to write
        compress: Whether to compress the output

    Returns:
        Number of records written
    """
    if compress and not file_path.suffix == ".gz":
        file_path = file_path.with_suffix(file_path.suffix + ".gz")

    if compress:
        file_handle = gzip.open(file_path, "wt", encoding="utf-8")
    else:
        file_handle = file_path.open("w", encoding="utf-8")

    try:
        count = 0
        for record in records:
            if JSON_LIB == "orjson":
                line = orjson.dumps(record).decode("utf-8")
            else:
                line = json.dumps(record, ensure_ascii=False)
            file_handle.write(line + "\n")
            count += 1
        return count
    finally:
        file_handle.close()
