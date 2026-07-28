#!/usr/bin/env python3
"""
partition_access_log.py

Partition large Apache access logs into UTC monthly buckets.

Purpose
-------

This is stage 1 of the FLCAC completed-download KPI pipeline.

The script:

1. Reads one or more source access logs line by line.
2. Extracts the Apache timestamp without fully parsing the request.
3. Converts the timestamp to UTC.
4. Writes the original line unchanged into:

       OUTPUT_DIR/YYYY/YYYY-MM.log

5. Writes lines with missing or invalid timestamps into:

       OUTPUT_DIR/unbucketed.log

6. Creates:

       OUTPUT_DIR/manifest.json

The script does not load the source log into memory and does not sort
events. Memory usage should remain effectively constant regardless of
source-log size.

Every input line is accounted for as either:

- bucketed into one monthly file, or
- written to unbucketed.log

Python requirement
------------------

Python 3.10 or newer is recommended.

No third-party packages are required.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import sys
import tempfile
import time

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Sequence


LOGGER = logging.getLogger("partition-access-log")

MANIFEST_VERSION = "flcac-access-log-partition-v1"

# Timestamp extraction is intentionally narrower than the complete Apache
# parser. Stage 1 only needs the timestamp required to select a month.
APACHE_TIMESTAMP_PATTERN = re.compile(
    rb"\[(?P<timestamp>"
    rb"\d{2}/[A-Za-z]{3}/\d{4}:"
    rb"\d{2}:\d{2}:\d{2} "
    rb"[+-]\d{4}"
    rb")\]"
)


@dataclass
class BucketStats:
    bucket: str
    output_file: str

    line_count: int = 0
    byte_count: int = 0

    earliest_timestamp: datetime | None = None
    latest_timestamp: datetime | None = None

    out_of_order_lines: int = 0
    previous_timestamp: datetime | None = None

    def record(
        self,
        timestamp: datetime,
        byte_count: int,
    ) -> None:
        self.line_count += 1
        self.byte_count += byte_count

        if (
            self.earliest_timestamp is None
            or timestamp < self.earliest_timestamp
        ):
            self.earliest_timestamp = timestamp

        if (
            self.latest_timestamp is None
            or timestamp > self.latest_timestamp
        ):
            self.latest_timestamp = timestamp

        if (
            self.previous_timestamp is not None
            and timestamp < self.previous_timestamp
        ):
            self.out_of_order_lines += 1

        self.previous_timestamp = timestamp

    def to_document(self) -> dict[str, object]:
        return {
            "bucket": self.bucket,
            "output_file": self.output_file,
            "line_count": self.line_count,
            "byte_count": self.byte_count,
            "earliest_timestamp": (
                self.earliest_timestamp.isoformat()
                if self.earliest_timestamp
                else None
            ),
            "latest_timestamp": (
                self.latest_timestamp.isoformat()
                if self.latest_timestamp
                else None
            ),
            "out_of_order_lines": self.out_of_order_lines,
        }


@dataclass
class SourceStats:
    source_file: str
    source_size_bytes: int

    line_count: int = 0
    byte_count_read: int = 0
    bucketed_lines: int = 0
    unbucketed_lines: int = 0

    missing_timestamp_lines: int = 0
    invalid_timestamp_lines: int = 0

    def to_document(self) -> dict[str, object]:
        return {
            "source_file": self.source_file,
            "source_size_bytes": self.source_size_bytes,
            "line_count": self.line_count,
            "byte_count_read": self.byte_count_read,
            "bucketed_lines": self.bucketed_lines,
            "unbucketed_lines": self.unbucketed_lines,
            "missing_timestamp_lines": (
                self.missing_timestamp_lines
            ),
            "invalid_timestamp_lines": (
                self.invalid_timestamp_lines
            ),
        }


@dataclass
class PartitionResult:
    source_stats: list[SourceStats]
    bucket_stats: dict[str, BucketStats]

    total_lines: int
    total_bytes_read: int
    bucketed_lines: int
    unbucketed_lines: int

    missing_timestamp_lines: int
    invalid_timestamp_lines: int

    unbucketed_bytes: int
    elapsed_seconds: float


def parse_args(
    argv: Sequence[str] | None = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Partition large Apache access logs into UTC monthly files."
        )
    )

    parser.add_argument(
        "logs",
        nargs="*",
        help="One or more Apache access-log files.",
    )

    parser.add_argument(
        "--output-dir",
        default="lca_log_data/raw",
        help=(
            "Directory for monthly logs and manifest.json. "
            "Default: lca_log_data/raw"
        ),
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Remove the output directory before partitioning. "
            "Without this option, a nonempty output directory causes "
            "the script to stop."
        ),
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=1_000_000,
        help=(
            "Report progress after this many lines. "
            "Use 0 to disable progress messages. "
            "Default: 1000000"
        ),
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run the built-in test instead of processing logs.",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )

    args = parser.parse_args(argv)

    if not args.self_test and not args.logs:
        parser.error(
            "Provide at least one access-log file or use --self-test."
        )

    if args.progress_every < 0:
        parser.error("--progress-every cannot be negative.")

    return args


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )


def parse_apache_timestamp(
    line: bytes,
) -> tuple[datetime | None, str | None]:
    """
    Extract and parse an Apache timestamp.

    Returns:
        (timestamp_utc, error_type)

    error_type is one of:

        None
        "missing_timestamp"
        "invalid_timestamp"
    """
    match = APACHE_TIMESTAMP_PATTERN.search(line)

    if not match:
        return None, "missing_timestamp"

    raw_timestamp = match.group("timestamp")

    try:
        timestamp_text = raw_timestamp.decode("ascii")

        parsed = datetime.strptime(
            timestamp_text,
            "%d/%b/%Y:%H:%M:%S %z",
        )

    except (UnicodeDecodeError, ValueError):
        return None, "invalid_timestamp"

    return parsed.astimezone(timezone.utc), None


def prepare_output_directory(
    output_dir: Path,
    *,
    overwrite: bool,
) -> None:
    if output_dir.exists():
        has_contents = any(output_dir.iterdir())

        if has_contents and not overwrite:
            raise ValueError(
                f"Output directory is not empty: {output_dir}. "
                "Use --overwrite to replace it."
            )

        if overwrite:
            LOGGER.warning(
                "Removing existing output directory: %s",
                output_dir,
            )

            shutil.rmtree(output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )


def open_bucket_handle(
    output_dir: Path,
    bucket: str,
    handles: dict[str, BinaryIO],
    bucket_stats: dict[str, BucketStats],
) -> BinaryIO:
    existing = handles.get(bucket)

    if existing is not None:
        return existing

    year = bucket[:4]
    year_dir = output_dir / year

    year_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = year_dir / f"{bucket}.log"

    handle = output_path.open("ab")
    handles[bucket] = handle

    bucket_stats[bucket] = BucketStats(
        bucket=bucket,
        output_file=str(
            output_path.relative_to(output_dir)
        ),
    )

    LOGGER.info(
        "Created monthly bucket %s",
        output_path,
    )

    return handle


def close_handles(
    handles: dict[str, BinaryIO],
) -> None:
    for handle in handles.values():
        try:
            handle.close()
        except OSError:
            LOGGER.exception(
                "Failed to close an output file"
            )


def partition_logs(
    source_paths: Sequence[Path],
    output_dir: Path,
    *,
    overwrite: bool,
    progress_every: int,
) -> PartitionResult:
    prepare_output_directory(
        output_dir,
        overwrite=overwrite,
    )

    source_stats: list[SourceStats] = []
    bucket_stats: dict[str, BucketStats] = {}

    bucket_handles: dict[str, BinaryIO] = {}
    unbucketed_handle: BinaryIO | None = None

    total_lines = 0
    total_bytes_read = 0
    bucketed_lines = 0
    unbucketed_lines = 0

    missing_timestamp_lines = 0
    invalid_timestamp_lines = 0
    unbucketed_bytes = 0

    started = time.monotonic()

    try:
        for source_path in source_paths:
            if not source_path.is_file():
                raise FileNotFoundError(
                    f"Source log not found: {source_path}"
                )

            source = SourceStats(
                source_file=str(source_path),
                source_size_bytes=(
                    source_path.stat().st_size
                ),
            )

            source_stats.append(source)

            LOGGER.info(
                "Reading %s (%s bytes)",
                source_path,
                f"{source.source_size_bytes:,}",
            )

            with source_path.open("rb") as input_handle:
                for line in input_handle:
                    line_size = len(line)

                    source.line_count += 1
                    source.byte_count_read += line_size

                    total_lines += 1
                    total_bytes_read += line_size

                    timestamp, error_type = (
                        parse_apache_timestamp(line)
                    )

                    if timestamp is None:
                        if unbucketed_handle is None:
                            unbucketed_path = (
                                output_dir / "unbucketed.log"
                            )

                            unbucketed_handle = (
                                unbucketed_path.open("ab")
                            )

                        unbucketed_handle.write(line)

                        source.unbucketed_lines += 1
                        unbucketed_lines += 1
                        unbucketed_bytes += line_size

                        if error_type == "missing_timestamp":
                            source.missing_timestamp_lines += 1
                            missing_timestamp_lines += 1
                        else:
                            source.invalid_timestamp_lines += 1
                            invalid_timestamp_lines += 1

                    else:
                        bucket = timestamp.strftime("%Y-%m")

                        output_handle = open_bucket_handle(
                            output_dir,
                            bucket,
                            bucket_handles,
                            bucket_stats,
                        )

                        output_handle.write(line)

                        stats = bucket_stats[bucket]

                        stats.record(
                            timestamp,
                            line_size,
                        )

                        source.bucketed_lines += 1
                        bucketed_lines += 1

                    if (
                        progress_every
                        and total_lines % progress_every == 0
                    ):
                        elapsed = (
                            time.monotonic() - started
                        )

                        rate = (
                            total_lines / elapsed
                            if elapsed > 0
                            else 0
                        )

                        LOGGER.info(
                            "Processed %s lines; "
                            "%s bucketed; "
                            "%s unbucketed; "
                            "%.0f lines/second",
                            f"{total_lines:,}",
                            f"{bucketed_lines:,}",
                            f"{unbucketed_lines:,}",
                            rate,
                        )

    finally:
        close_handles(bucket_handles)

        if unbucketed_handle is not None:
            unbucketed_handle.close()

    elapsed_seconds = time.monotonic() - started

    return PartitionResult(
        source_stats=source_stats,
        bucket_stats=bucket_stats,
        total_lines=total_lines,
        total_bytes_read=total_bytes_read,
        bucketed_lines=bucketed_lines,
        unbucketed_lines=unbucketed_lines,
        missing_timestamp_lines=(
            missing_timestamp_lines
        ),
        invalid_timestamp_lines=(
            invalid_timestamp_lines
        ),
        unbucketed_bytes=unbucketed_bytes,
        elapsed_seconds=elapsed_seconds,
    )


def build_manifest(
    result: PartitionResult,
    output_dir: Path,
) -> dict[str, object]:
    buckets = [
        result.bucket_stats[bucket].to_document()
        for bucket in sorted(result.bucket_stats)
    ]

    bucket_line_sum = sum(
        stats.line_count
        for stats in result.bucket_stats.values()
    )

    bucket_byte_sum = sum(
        stats.byte_count
        for stats in result.bucket_stats.values()
    )

    accounted_lines = (
        bucket_line_sum
        + result.unbucketed_lines
    )

    accounted_bytes = (
        bucket_byte_sum
        + result.unbucketed_bytes
    )

    return {
        "manifest_version": MANIFEST_VERSION,
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "partition_timezone": "UTC",
        "output_directory": str(output_dir),
        "source_files": [
            source.to_document()
            for source in result.source_stats
        ],
        "totals": {
            "source_file_count": len(
                result.source_stats
            ),
            "input_lines": result.total_lines,
            "input_bytes": result.total_bytes_read,
            "bucketed_lines": result.bucketed_lines,
            "unbucketed_lines": (
                result.unbucketed_lines
            ),
            "missing_timestamp_lines": (
                result.missing_timestamp_lines
            ),
            "invalid_timestamp_lines": (
                result.invalid_timestamp_lines
            ),
            "monthly_bucket_count": len(
                result.bucket_stats
            ),
            "monthly_bucket_bytes": (
                bucket_byte_sum
            ),
            "unbucketed_bytes": (
                result.unbucketed_bytes
            ),
            "elapsed_seconds": round(
                result.elapsed_seconds,
                3,
            ),
        },
        "validation": {
            "input_lines_equal_accounted_lines": (
                result.total_lines
                == accounted_lines
            ),
            "input_bytes_equal_accounted_bytes": (
                result.total_bytes_read
                == accounted_bytes
            ),
            "accounted_lines": accounted_lines,
            "accounted_bytes": accounted_bytes,
        },
        "unbucketed": {
            "output_file": (
                "unbucketed.log"
                if result.unbucketed_lines
                else None
            ),
            "line_count": (
                result.unbucketed_lines
            ),
            "byte_count": (
                result.unbucketed_bytes
            ),
        },
        "buckets": buckets,
    }


def write_manifest(
    output_dir: Path,
    manifest: dict[str, object],
) -> Path:
    manifest_path = output_dir / "manifest.json"
    temporary_path = output_dir / "manifest.json.tmp"

    with temporary_path.open(
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            manifest,
            handle,
            indent=2,
            sort_keys=True,
        )

        handle.write("\n")

    temporary_path.replace(manifest_path)

    LOGGER.info(
        "Wrote manifest to %s",
        manifest_path,
    )

    return manifest_path


def run_self_test() -> int:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)

        source_path = root / "sample.log"
        output_dir = root / "raw"

        lines = [
            (
                b'example.org 192.0.2.1 - - '
                b'[30/Sep/2024:23:59:59 -0400] '
                b'"GET /browse/process-1 HTTP/1.1" '
                b'200 100 "-" "agent" "-" 0.1\n'
            ),
            (
                b'example.org 192.0.2.1 - - '
                b'[15/Jan/2025:12:00:00 +0000] '
                b'"GET /browse/process-2 HTTP/1.1" '
                b'200 100 "-" "agent" "-" 0.1\n'
            ),
            (
                b'example.org 192.0.2.1 - - '
                b'[15/Jan/2025:11:00:00 +0000] '
                b'"GET /browse/process-3 HTTP/1.1" '
                b'200 100 "-" "agent" "-" 0.1\n'
            ),
            b"this line has no Apache timestamp\n",
        ]

        with source_path.open("wb") as handle:
            for line in lines:
                handle.write(line)

        result = partition_logs(
            [source_path],
            output_dir,
            overwrite=False,
            progress_every=0,
        )

        manifest = build_manifest(
            result,
            output_dir,
        )

        write_manifest(
            output_dir,
            manifest,
        )

        october_path = (
            output_dir
            / "2024"
            / "2024-10.log"
        )

        january_path = (
            output_dir
            / "2025"
            / "2025-01.log"
        )

        unbucketed_path = (
            output_dir
            / "unbucketed.log"
        )

        checks = [
            (
                result.total_lines == 4,
                (
                    "expected 4 input lines, got "
                    f"{result.total_lines}"
                ),
            ),
            (
                result.bucketed_lines == 3,
                (
                    "expected 3 bucketed lines, got "
                    f"{result.bucketed_lines}"
                ),
            ),
            (
                result.unbucketed_lines == 1,
                (
                    "expected 1 unbucketed line, got "
                    f"{result.unbucketed_lines}"
                ),
            ),
            (
                october_path.is_file(),
                "expected UTC October 2024 bucket",
            ),
            (
                january_path.is_file(),
                "expected January 2025 bucket",
            ),
            (
                unbucketed_path.is_file(),
                "expected unbucketed.log",
            ),
            (
                result.bucket_stats[
                    "2025-01"
                ].out_of_order_lines == 1,
                "expected one out-of-order January line",
            ),
            (
                manifest["validation"][
                    "input_lines_equal_accounted_lines"
                ] is True,
                "line accounting validation failed",
            ),
            (
                manifest["validation"][
                    "input_bytes_equal_accounted_bytes"
                ] is True,
                "byte accounting validation failed",
            ),
        ]

        failures = [
            message
            for passed, message in checks
            if not passed
        ]

        if failures:
            for failure in failures:
                print(
                    f"SELF-TEST FAILED: {failure}",
                    file=sys.stderr,
                )

            return 1

        print("SELF-TEST PASSED")
        print(
            json.dumps(
                manifest,
                indent=2,
                sort_keys=True,
            )
        )

        return 0


def main(
    argv: Sequence[str] | None = None,
) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    if args.self_test:
        return run_self_test()

    try:
        source_paths = [
            Path(path_text)
            for path_text in args.logs
        ]

        output_dir = Path(args.output_dir)

        result = partition_logs(
            source_paths,
            output_dir,
            overwrite=args.overwrite,
            progress_every=args.progress_every,
        )

        manifest = build_manifest(
            result,
            output_dir,
        )

        manifest_path = write_manifest(
            output_dir,
            manifest,
        )

        validation = manifest["validation"]

        if not (
            validation[
                "input_lines_equal_accounted_lines"
            ]
            and validation[
                "input_bytes_equal_accounted_bytes"
            ]
        ):
            raise RuntimeError(
                "Partition validation failed. "
                "Input and output accounting do not match."
            )

        LOGGER.info(
            "Partitioning complete: "
            "%s input lines, "
            "%s monthly lines, "
            "%s unbucketed lines, "
            "%s monthly buckets",
            f"{result.total_lines:,}",
            f"{result.bucketed_lines:,}",
            f"{result.unbucketed_lines:,}",
            f"{len(result.bucket_stats):,}",
        )

        print(
            json.dumps(
                {
                    "status": "complete",
                    "manifest": str(
                        manifest_path
                    ),
                    "input_lines": (
                        result.total_lines
                    ),
                    "bucketed_lines": (
                        result.bucketed_lines
                    ),
                    "unbucketed_lines": (
                        result.unbucketed_lines
                    ),
                    "monthly_bucket_count": len(
                        result.bucket_stats
                    ),
                },
                indent=2,
                sort_keys=True,
            )
        )

        return 0

    except (
        FileNotFoundError,
        OSError,
        ValueError,
        RuntimeError,
    ) as exc:
        LOGGER.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
