from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional


@dataclass
class PreProcessingResult:
    source_exists: bool = False
    source_readable: bool = False

    total_lines: int = 0
    timestamped_lines: int = 0
    malformed_lines: int = 0

    earliest_timestamp: Optional[datetime] = None
    latest_timestamp: Optional[datetime] = None

    before_start: int = 0
    in_window: int = 0
    at_or_after_end: int = 0

    passed: bool = False
    errors: list[str] = field(default_factory=list)

import re
APACHE_TIMESTAMP_RE = re.compile(r"\[([^\]]+)\]")

def parse_apache_timestamp(line: str) -> Optional[datetime]:
    match = APACHE_TIMESTAMP_RE.search(line)

    if not match:
        return None

    try:
        return datetime.strptime(
            match.group(1),
            "%d/%b/%Y:%H:%M:%S %z",
        )
    except ValueError:
        return None


def validate_source(
    source_path: str,
    start_date: datetime,
    end_date: datetime,
    timestamp_parser: Callable[[str], Optional[datetime]],
) -> PreProcessingResult:
    result = PreProcessingResult()

    path = Path(source_path)

    if not path.is_file():
        result.errors.append(f"Source file does not exist: {source_path}")
        return result

    result.source_exists = True

    try:
        with path.open("rt", errors="replace") as source:
            result.source_readable = True

            for line in source:
                result.total_lines += 1

                timestamp = timestamp_parser(line)

                if timestamp is None:
                    result.malformed_lines += 1
                    continue

                result.timestamped_lines += 1

                if (
                    result.earliest_timestamp is None
                    or timestamp < result.earliest_timestamp
                ):
                    result.earliest_timestamp = timestamp

                if (
                    result.latest_timestamp is None
                    or timestamp > result.latest_timestamp
                ):
                    result.latest_timestamp = timestamp

                if timestamp < start_date:
                    result.before_start += 1
                elif timestamp >= end_date:
                    result.at_or_after_end += 1
                else:
                    result.in_window += 1

    except OSError as exc:
        result.errors.append(f"Unable to read source file: {exc}")
        return result

    if result.total_lines == 0:
        result.errors.append("Source file is empty.")

    if result.timestamped_lines == 0:
        result.errors.append("No parseable timestamps found.")

    result.passed = len(result.errors) == 0

    return result