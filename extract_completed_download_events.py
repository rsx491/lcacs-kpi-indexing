#!/usr/bin/env python3
"""
extract_completed_download_events.py

Stage 2 of the FLCAC Completed Download KPI pipeline.

The script reads the UTC monthly raw buckets produced by
``partition_access_log.py``, parses each line with the canonical parser in
``index_completed_downloads.py``, retains only completed-download-relevant
endpoint events, and writes chronologically sorted monthly JSONL files.

Relevant event types are the canonical categories:

- prepare
- token_retrieval
- browse_dataset
- repository_file
- repository_export

Repository-export paths are retained and enriched for repository-level
reporting.

Failed retrievals are retained here because success filtering belongs to the
final KPI processor, not to this extraction stage.

Sorting is bounded-memory. Retained events are sorted in chunks and merged
using the deterministic key:

    timestamp UTC, source-file order, source line number

No third-party packages are required.
"""

from __future__ import annotations

import argparse
import heapq
import importlib.util
import json
import logging
import re
import shutil
import sys
import tempfile
import time
import urllib.parse

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence, TextIO


LOGGER = logging.getLogger(
    "extract-completed-download-events"
)

MANIFEST_VERSION = (
    "flcac-completed-download-extraction-v1"
)

DEFAULT_CHUNK_EVENTS = 100_000
DEFAULT_PROGRESS_EVERY = 1_000_000

REPOSITORY_EXPORT = "repository_export"


@dataclass
class BucketStats:
    bucket: str
    input_file: str
    output_file: str
    source_file_order: int

    input_lines: int = 0
    input_bytes: int = 0
    blank_lines: int = 0
    parsed_events: int = 0
    malformed_lines: int = 0
    irrelevant_events: int = 0
    excluded_repository_exports: int = 0
    retained_events: int = 0

    retained_by_type: dict[str, int] = field(
        default_factory=dict
    )

    filtered_input_out_of_order: int = 0

    earliest_retained_timestamp: (
        datetime | None
    ) = None

    latest_retained_timestamp: (
        datetime | None
    ) = None

    chunk_count: int = 0
    output_bytes: int = 0

    def to_document(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "input_file": self.input_file,
            "output_file": self.output_file,
            "source_file_order": (
                self.source_file_order
            ),
            "input_lines": self.input_lines,
            "input_bytes": self.input_bytes,
            "blank_lines": self.blank_lines,
            "parsed_events": self.parsed_events,
            "malformed_lines": (
                self.malformed_lines
            ),
            "irrelevant_events": (
                self.irrelevant_events
            ),
            "excluded_repository_exports": (
                self.excluded_repository_exports
            ),
            "retained_events": (
                self.retained_events
            ),
            "retained_by_type": dict(
                sorted(
                    self.retained_by_type.items()
                )
            ),
            "filtered_input_out_of_order": (
                self.filtered_input_out_of_order
            ),
            "earliest_retained_timestamp": (
                isoformat_or_none(
                    self.earliest_retained_timestamp
                )
            ),
            "latest_retained_timestamp": (
                isoformat_or_none(
                    self.latest_retained_timestamp
                )
            ),
            "chunk_count": self.chunk_count,
            "output_bytes": self.output_bytes,
            "validation": {
                "input_lines_accounted_for": (
                    self.input_lines
                    == (
                        self.blank_lines
                        + self.malformed_lines
                        + self.irrelevant_events
                        + self.retained_events
                    )
                ),
                "parsed_events_accounted_for": (
                    self.parsed_events
                    == (
                        self.irrelevant_events
                        + self.retained_events
                    )
                ),
            },
        }


def isoformat_or_none(
    value: datetime | None,
) -> str | None:
    if value is None:
        return None

    return value.isoformat()


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=(
            logging.DEBUG
            if verbose
            else logging.INFO
        ),
        format="%(levelname)s: %(message)s",
    )


def parse_args(
    argv: Sequence[str] | None = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract and externally sort "
            "completed-download-relevant events "
            "from monthly Apache access-log "
            "buckets."
        )
    )

    parser.add_argument(
        "--input-dir",
        default="lca_log_data/raw",
        help=(
            "Raw partition directory containing "
            "manifest.json and monthly bucket "
            "files. Default: lca_log_data/raw"
        ),
    )

    parser.add_argument(
        "--output-dir",
        default=(
            "lca_log_data/filtered/"
            "completed_downloads"
        ),
        help=(
            "Output directory for monthly JSONL "
            "and extraction manifest. Default: "
            "lca_log_data/filtered/"
            "completed_downloads"
        ),
    )

    parser.add_argument(
        "--canonical-script",
        default=str(
            Path(__file__).with_name(
                "index_completed_downloads.py"
            )
        ),
        help=(
            "Path to the canonical "
            "index_completed_downloads.py parser. "
            "Default: sibling file"
        ),
    )

    parser.add_argument(
        "--chunk-events",
        type=int,
        default=DEFAULT_CHUNK_EVENTS,
        help=(
            "Maximum retained events held in "
            "memory before writing a sorted "
            f"chunk. Default: "
            f"{DEFAULT_CHUNK_EVENTS}"
        ),
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help=(
            "Report progress after this many raw "
            "lines per bucket; 0 disables. "
            f"Default: {DEFAULT_PROGRESS_EVERY}"
        ),
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Replace a nonempty output directory."
        ),
    )

    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help=(
            "Keep sorted chunk files for "
            "troubleshooting."
        ),
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "Run the built-in test instead of "
            "processing production data."
        ),
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )

    args = parser.parse_args(argv)

    if args.chunk_events <= 0:
        parser.error(
            "--chunk-events must be greater "
            "than zero"
        )

    if args.progress_every < 0:
        parser.error(
            "--progress-every cannot be negative"
        )

    return args


def load_canonical_module(path: Path) -> Any:
    if not path.is_file():
        raise FileNotFoundError(
            f"Canonical script not found: {path}"
        )

    spec = importlib.util.spec_from_file_location(
        (
            "flcac_index_completed_downloads_"
            "canonical"
        ),
        path,
    )

    if spec is None or spec.loader is None:
        raise ImportError(
            f"Could not load canonical script: "
            f"{path}"
        )

    module = importlib.util.module_from_spec(spec)

    sys.modules[spec.name] = module

    spec.loader.exec_module(module)

    required = (
        "parse_log_line",
        "classify_event",
        "PREPARE",
        "TOKEN_RETRIEVAL",
        "BROWSE_DATASET",
        "REPOSITORY_FILE",
    )

    missing = [
        name
        for name in required
        if not hasattr(module, name)
    ]

    if missing:
        raise ImportError(
            "Canonical script is missing "
            "required names: "
            + ", ".join(missing)
        )

    return module


def prepare_output_directory(
    output_dir: Path,
    overwrite: bool,
) -> None:
    if (
        output_dir.exists()
        and any(output_dir.iterdir())
    ):
        if not overwrite:
            raise ValueError(
                "Output directory is not empty: "
                f"{output_dir}. Use --overwrite "
                "to replace it."
            )

        LOGGER.warning(
            "Removing existing output directory: "
            "%s",
            output_dir,
        )

        shutil.rmtree(output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )


def load_partition_manifest(
    input_dir: Path,
) -> dict[str, Any]:
    manifest_path = (
        input_dir / "manifest.json"
    )

    if not manifest_path.is_file():
        raise FileNotFoundError(
            "Partition manifest not found: "
            f"{manifest_path}"
        )

    with manifest_path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        manifest = json.load(handle)

    buckets = manifest.get("buckets")

    if (
        not isinstance(buckets, list)
        or not buckets
    ):
        raise ValueError(
            "No bucket list found in "
            f"{manifest_path}"
        )

    return manifest


def event_sort_key(
    document: dict[str, Any],
) -> tuple[str, int, int]:
    return (
        str(document["timestamp"]),
        int(document["source_file_order"]),
        int(document["source_line"]),
    )


def normalize_resource_type(
    value: str | None,
) -> str | None:
    if not value:
        return None

    normalized = (
        value.strip()
        .replace("-", "_")
        .upper()
    )

    aliases = {
        "PROCESSES": "PROCESS",
        "FLOWS": "FLOW",
        "FLOWPROPERTY": "FLOW_PROPERTY",
        "FLOW_PROPERTIES": "FLOW_PROPERTY",
        "UNITGROUP": "UNIT_GROUP",
        "UNIT_GROUPS": "UNIT_GROUP",
        "ACTORS": "ACTOR",
        "SOURCES": "SOURCE",
        "CATEGORIES": "CATEGORY",
        "PRODUCTSYSTEM": "PRODUCT_SYSTEM",
        "PRODUCT_SYSTEMS": "PRODUCT_SYSTEM",
        "REPOSITORY": "REPOSITORY",
    }

    return aliases.get(
        normalized,
        normalized,
    )


def parse_prepare_path(
    path: str,
) -> dict[str, str]:
    match = re.search(
        r"/download/json1?/prepare/"
        r"(?P<group>[^/?#]+)/"
        r"(?P<repository>[^/?#]+)"
        r"(?:/"
        r"(?P<resource_type>[^/?#]+)/"
        r"(?P<resource_id>[^/?#]+)"
        r")?"
        r"(?:[/?#]|$)",
        path,
        re.IGNORECASE,
    )

    if not match:
        return {}

    result = {
        "group": urllib.parse.unquote(
            match.group("group")
        ).strip(),
        "repository": urllib.parse.unquote(
            match.group("repository")
        ).strip(),
    }

    resource_type = match.group(
        "resource_type"
    )

    if resource_type:
        normalized_type = (
            normalize_resource_type(
                urllib.parse.unquote(
                    resource_type
                )
            )
        )

        if normalized_type:
            result[
                "resource_type"
            ] = normalized_type

    resource_id = match.group("resource_id")

    if resource_id:
        result["resource_id"] = (
            urllib.parse.unquote(
                resource_id
            ).strip()
        )

    return result
def is_repository_export_path(
    path: str,
) -> bool:
    return bool(
        re.search(
            (
                r"/download/json1?/"
                r"repository_[^/?#]+"
            ),
            path,
            re.IGNORECASE,
        )
    )


def parse_repository_export_path(
    path: str,
) -> dict[str, str]:
    match = re.search(
        r"/download/json1?/"
        r"repository_"
        r"(?P<group>[^@/?#]+)"
        r"@(?P<repository>[^@/?#]+)"
        r"@(?P<download_identifier>[^/?#]+)",
        path,
        re.IGNORECASE,
    )

    if not match:
        return {}

    download_identifier = (
        urllib.parse.unquote(
            match.group(
                "download_identifier"
            )
        ).strip()
    )

    return {
        "group": urllib.parse.unquote(
            match.group("group")
        ).strip(),
        "repository": urllib.parse.unquote(
            match.group("repository")
        ).strip(),
        "resource_type": "REPOSITORY",
        "resource_id": download_identifier,
        "download_identifier": (
            download_identifier
        ),
    }


def parse_referer_attribution(
    referer: str | None,
) -> tuple[dict[str, str], str]:
    if not referer or referer == "-":
        return {}, ""

    parsed = urllib.parse.urlsplit(
        referer
    )

    path = parsed.path

    dataset_match = re.search(
        r"/lca-collaboration/"
        r"(?P<group>[^/?#]+)/"
        r"(?P<repository>[^/?#]+)/"
        r"dataset/"
        r"(?P<resource_type>[^/?#]+)/"
        r"(?P<resource_id>[^/?#]+)",
        path,
        re.IGNORECASE,
    )

    if dataset_match:
        resource_type = (
            normalize_resource_type(
                urllib.parse.unquote(
                    dataset_match.group(
                        "resource_type"
                    )
                )
            )
        )

        result = {
            "group": urllib.parse.unquote(
                dataset_match.group("group")
            ).strip(),
            "repository": (
                urllib.parse.unquote(
                    dataset_match.group(
                        "repository"
                    )
                ).strip()
            ),
            "resource_id": (
                urllib.parse.unquote(
                    dataset_match.group(
                        "resource_id"
                    )
                ).strip()
            ),
        }

        if resource_type:
            result[
                "resource_type"
            ] = resource_type

        return (
            result,
            "dataset_referer_path",
        )

    query = urllib.parse.parse_qs(
        parsed.query,
        keep_blank_values=True,
    )

    repository_id_values = query.get(
        "repositoryId",
        [],
    )

    repository_id = (
        repository_id_values[0]
        if repository_id_values
        else ""
    )

    if repository_id:
        decoded = urllib.parse.unquote(
            repository_id
        ).strip("/")

        if "/" in decoded:
            group, repository = (
                decoded.split("/", 1)
            )

            return (
                {
                    "group": group,
                    "repository": repository,
                },
                "referer_repository_id",
            )

    group_values = query.get(
        "group",
        [],
    )

    group = (
        group_values[0]
        if group_values
        else ""
    )

    if group:
        return (
            {
                "group": (
                    urllib.parse.unquote(
                        group
                    ).strip()
                )
            },
            "referer_group_parameter",
        )

    return {}, ""


def enrich_event_document(
    document: dict[str, Any],
) -> dict[str, Any]:
    path = str(
        document.get("path") or ""
    )

    event_type = str(
        document.get("event_type") or ""
    )

    values: dict[str, str] = {}
    attribution_source = ""

    if event_type == REPOSITORY_EXPORT:
        values = (
            parse_repository_export_path(
                path
            )
        )

        if values:
            attribution_source = (
                "repository_export_path"
            )

    elif event_type == "prepare":
        values = parse_prepare_path(path)

        if values:
            attribution_source = (
                "prepare_request_path"
            )

    if not values:
        (
            values,
            attribution_source,
        ) = parse_referer_attribution(
            document.get("referer")
        )

    if values:
        for key, value in values.items():
            if value:
                document[key] = value

    group = document.get("group")
    repository = document.get(
        "repository"
    )

    if group and repository:
        document["repository_key"] = (
            f"{group}/{repository}"
        )

        document[
            "attribution_status"
        ] = "attributed"

        document[
            "attribution_confidence"
        ] = "high"

    elif group:
        document[
            "attribution_status"
        ] = "group_only"

        document[
            "attribution_confidence"
        ] = "partial"

    else:
        document[
            "attribution_status"
        ] = "unattributed"

        document[
            "attribution_confidence"
        ] = "none"

    document["attribution_source"] = (
        attribution_source or None
    )

    document["access_type"] = (
        "public"
        if "/ws/public/" in path
        else "unknown"
    )

    user_id = document.get("user_id")

    document["is_authenticated"] = bool(
        user_id
        and user_id != "-"
    )

    document["resource_type"] = (
        normalize_resource_type(
            document.get("resource_type")
        )
    )

    return document


def parsed_event_document(
    event: Any,
    event_type: str,
    source_file_order: int,
) -> dict[str, Any]:
    document = asdict(event)

    timestamp = event.timestamp.astimezone(
        timezone.utc
    )

    document["timestamp"] = (
        timestamp.isoformat()
    )

    document["event_type"] = event_type

    document["source_file_order"] = (
        source_file_order
    )

    return enrich_event_document(document)


def write_sorted_chunk(
    records: list[dict[str, Any]],
    chunk_dir: Path,
    chunk_number: int,
) -> Path:
    records.sort(
        key=event_sort_key
    )

    path = (
        chunk_dir
        / f"chunk-{chunk_number:06d}.jsonl"
    )

    with path.open(
        "w",
        encoding="utf-8",
    ) as handle:
        for document in records:
            handle.write(
                json.dumps(
                    document,
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )

            handle.write("\n")

    return path


def read_chunk_record(
    handle: TextIO,
) -> dict[str, Any] | None:
    line = handle.readline()

    if not line:
        return None

    value = json.loads(line)

    if not isinstance(value, dict):
        raise ValueError(
            "Sorted chunk contains a "
            "non-object JSON value"
        )

    return value


def merge_sorted_chunks(
    chunk_paths: Sequence[Path],
    output_path: Path,
) -> int:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    handles: list[TextIO] = []

    heap: list[
        tuple[
            tuple[str, int, int],
            int,
            dict[str, Any],
        ]
    ] = []

    written = 0

    try:
        for index, path in enumerate(
            chunk_paths
        ):
            handle = path.open(
                "r",
                encoding="utf-8",
            )

            handles.append(handle)

            record = read_chunk_record(
                handle
            )

            if record is not None:
                heapq.heappush(
                    heap,
                    (
                        event_sort_key(record),
                        index,
                        record,
                    ),
                )

        temporary_path = (
            output_path.with_suffix(
                output_path.suffix + ".tmp"
            )
        )

        with temporary_path.open(
            "w",
            encoding="utf-8",
        ) as output:
            while heap:
                (
                    _,
                    index,
                    record,
                ) = heapq.heappop(heap)

                output.write(
                    json.dumps(
                        record,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                )

                output.write("\n")

                written += 1

                next_record = (
                    read_chunk_record(
                        handles[index]
                    )
                )

                if next_record is not None:
                    heapq.heappush(
                        heap,
                        (
                            event_sort_key(
                                next_record
                            ),
                            index,
                            next_record,
                        ),
                    )

        temporary_path.replace(
            output_path
        )

    finally:
        for handle in handles:
            handle.close()

    return written


def process_bucket(
    *,
    canonical: Any,
    bucket: str,
    source_path: Path,
    source_file_order: int,
    output_dir: Path,
    chunk_events: int,
    progress_every: int,
    keep_temp: bool,
) -> BucketStats:
    year = bucket[:4]

    output_path = (
        output_dir
        / year
        / f"{bucket}.jsonl"
    )

    relative_input = str(source_path)

    relative_output = str(
        output_path.relative_to(
            output_dir
        )
    )

    stats = BucketStats(
        bucket=bucket,
        input_file=relative_input,
        output_file=relative_output,
        source_file_order=(
            source_file_order
        ),
    )

    chunk_root = (
        output_dir
        / ".tmp"
        / bucket
    )

    chunk_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    chunk_paths: list[Path] = []

    records: list[
        dict[str, Any]
    ] = []

    previous_retained_timestamp: (
        datetime | None
    ) = None

    started = time.monotonic()

    LOGGER.info(
        "Reading bucket %s from %s",
        bucket,
        source_path,
    )

    with source_path.open("rb") as handle:
        for (
            line_number,
            raw_line,
        ) in enumerate(
            handle,
            start=1,
        ):
            stats.input_lines += 1
            stats.input_bytes += len(
                raw_line
            )

            line = raw_line.decode(
                "utf-8",
                errors="replace",
            )

            if not line.strip():
                stats.blank_lines += 1
                continue

            try:
                event = (
                    canonical.parse_log_line(
                        line,
                        source_file=str(
                            source_path
                        ),
                        source_line=(
                            line_number
                        ),
                    )
                )

            except (
                ValueError,
                json.JSONDecodeError,
            ) as exc:
                stats.malformed_lines += 1

                LOGGER.debug(
                    "Malformed line %s:%d: %s",
                    source_path,
                    line_number,
                    exc,
                )

                continue

            stats.parsed_events += 1

            if is_repository_export_path(
                event.path
            ):
                event_type = REPOSITORY_EXPORT
            else:
                event_type = (
                    canonical.classify_event(
                        event.path
                    )
                )

            if event_type is None:
                stats.irrelevant_events += 1
                continue

            stats.retained_events += 1

            stats.retained_by_type[
                event_type
            ] = (
                stats.retained_by_type.get(
                    event_type,
                    0,
                )
                + 1
            )

            timestamp = (
                event.timestamp.astimezone(
                    timezone.utc
                )
            )

            if (
                previous_retained_timestamp
                is not None
                and timestamp
                < previous_retained_timestamp
            ):
                (
                    stats
                    .filtered_input_out_of_order
                ) += 1

            previous_retained_timestamp = (
                timestamp
            )

            if (
                stats
                .earliest_retained_timestamp
                is None
                or timestamp
                < stats
                .earliest_retained_timestamp
            ):
                (
                    stats
                    .earliest_retained_timestamp
                ) = timestamp

            if (
                stats
                .latest_retained_timestamp
                is None
                or timestamp
                > stats
                .latest_retained_timestamp
            ):
                (
                    stats
                    .latest_retained_timestamp
                ) = timestamp

            records.append(
                parsed_event_document(
                    event,
                    event_type,
                    source_file_order,
                )
            )

            if len(records) >= chunk_events:
                chunk_paths.append(
                    write_sorted_chunk(
                        records,
                        chunk_root,
                        len(chunk_paths) + 1,
                    )
                )

                records = []

            if (
                progress_every
                and stats.input_lines
                % progress_every
                == 0
            ):
                elapsed = (
                    time.monotonic()
                    - started
                )

                rate = (
                    stats.input_lines
                    / elapsed
                    if elapsed
                    else 0.0
                )

                LOGGER.info(
                    (
                        "%s: processed %s lines; "
                        "retained %s; malformed %s; "
                        "%.0f lines/second"
                    ),
                    bucket,
                    f"{stats.input_lines:,}",
                    f"{stats.retained_events:,}",
                    f"{stats.malformed_lines:,}",
                    rate,
                )
    if records:
        chunk_paths.append(
            write_sorted_chunk(
                records,
                chunk_root,
                len(chunk_paths) + 1,
            )
        )

    stats.chunk_count = len(
        chunk_paths
    )

    if chunk_paths:
        written = merge_sorted_chunks(
            chunk_paths,
            output_path,
        )

        if written != stats.retained_events:
            raise RuntimeError(
                "Output count mismatch for "
                f"{bucket}: expected "
                f"{stats.retained_events}, "
                f"wrote {written}"
            )

    else:
        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path.write_text(
            "",
            encoding="utf-8",
        )

    stats.output_bytes = (
        output_path.stat().st_size
    )

    if not keep_temp:
        shutil.rmtree(
            chunk_root,
            ignore_errors=True,
        )

    LOGGER.info(
        (
            "%s complete: %s input lines, "
            "%s retained events, %s chunks"
        ),
        bucket,
        f"{stats.input_lines:,}",
        f"{stats.retained_events:,}",
        f"{stats.chunk_count:,}",
    )

    return stats


def build_manifest(
    *,
    input_dir: Path,
    output_dir: Path,
    canonical_script: Path,
    partition_manifest: dict[str, Any],
    bucket_stats: list[BucketStats],
    elapsed_seconds: float,
    chunk_events: int,
) -> dict[str, Any]:
    total_input_lines = sum(
        item.input_lines
        for item in bucket_stats
    )

    total_input_bytes = sum(
        item.input_bytes
        for item in bucket_stats
    )

    total_blank = sum(
        item.blank_lines
        for item in bucket_stats
    )

    total_parsed = sum(
        item.parsed_events
        for item in bucket_stats
    )

    total_malformed = sum(
        item.malformed_lines
        for item in bucket_stats
    )

    total_irrelevant = sum(
        item.irrelevant_events
        for item in bucket_stats
    )

    total_exports = sum(
        item.excluded_repository_exports
        for item in bucket_stats
    )

    total_retained = sum(
        item.retained_events
        for item in bucket_stats
    )

    total_output_bytes = sum(
        item.output_bytes
        for item in bucket_stats
    )

    total_chunks = sum(
        item.chunk_count
        for item in bucket_stats
    )

    retained_by_type: dict[str, int] = {}

    for item in bucket_stats:
        for (
            event_type,
            count,
        ) in item.retained_by_type.items():
            retained_by_type[
                event_type
            ] = (
                retained_by_type.get(
                    event_type,
                    0,
                )
                + count
            )

    source_totals = (
        partition_manifest.get(
            "totals",
            {},
        )
    )

    expected_lines = (
        source_totals.get(
            "bucketed_lines"
        )
    )

    expected_bytes = (
        source_totals.get(
            "monthly_bucket_bytes"
        )
    )

    return {
        "manifest_version": (
            MANIFEST_VERSION
        ),
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "input_directory": str(
            input_dir
        ),
        "output_directory": str(
            output_dir
        ),
        "canonical_script": str(
            canonical_script
        ),
        "sort_key": [
            "timestamp",
            "source_file_order",
            "source_line",
        ],
        "chunk_events": chunk_events,
        "totals": {
            "bucket_count": len(
                bucket_stats
            ),
            "input_lines": (
                total_input_lines
            ),
            "input_bytes": (
                total_input_bytes
            ),
            "blank_lines": total_blank,
            "parsed_events": (
                total_parsed
            ),
            "malformed_lines": (
                total_malformed
            ),
            "irrelevant_events": (
                total_irrelevant
            ),
            "excluded_repository_exports": (
                total_exports
            ),
            "retained_events": (
                total_retained
            ),
            "retained_by_type": dict(
                sorted(
                    retained_by_type.items()
                )
            ),
            "filtered_input_out_of_order": sum(
                item.filtered_input_out_of_order
                for item in bucket_stats
            ),
            "sort_chunk_count": (
                total_chunks
            ),
            "output_bytes": (
                total_output_bytes
            ),
            "elapsed_seconds": round(
                elapsed_seconds,
                3,
            ),
        },
        "validation": {
            "input_lines_accounted_for": (
                total_input_lines
                == (
                    total_blank
                    + total_malformed
                    + total_irrelevant
                    + total_retained
                )
            ),
            "parsed_events_accounted_for": (
                total_parsed
                == (
                    total_irrelevant
                    + total_retained
                )
            ),
            "matches_partition_manifest_lines": (
                expected_lines is None
                or total_input_lines
                == expected_lines
            ),
            "matches_partition_manifest_bytes": (
                expected_bytes is None
                or total_input_bytes
                == expected_bytes
            ),
            "all_bucket_validations_pass": all(
                (
                    item.to_document()[
                        "validation"
                    ][
                        "input_lines_accounted_for"
                    ]
                    and item.to_document()[
                        "validation"
                    ][
                        "parsed_events_accounted_for"
                    ]
                )
                for item in bucket_stats
            ),
        },
        "buckets": [
            item.to_document()
            for item in bucket_stats
        ],
    }


def write_manifest(
    output_dir: Path,
    manifest: dict[str, Any],
) -> Path:
    path = (
        output_dir
        / "manifest.json"
    )

    temporary = (
        output_dir
        / "manifest.json.tmp"
    )

    with temporary.open(
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

    temporary.replace(path)

    return path


def run_pipeline(
    args: argparse.Namespace,
) -> int:
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    canonical_script = Path(
        args.canonical_script
    )

    canonical = load_canonical_module(
        canonical_script
    )

    partition_manifest = (
        load_partition_manifest(
            input_dir
        )
    )

    prepare_output_directory(
        output_dir,
        args.overwrite,
    )

    raw_buckets = (
        partition_manifest["buckets"]
    )

    sorted_buckets = sorted(
        raw_buckets,
        key=lambda item: str(
            item["bucket"]
        ),
    )

    results: list[BucketStats] = []

    started = time.monotonic()

    for (
        source_file_order,
        item,
    ) in enumerate(sorted_buckets):
        bucket = str(item["bucket"])

        source_path = (
            input_dir
            / str(item["output_file"])
        )

        if not source_path.is_file():
            raise FileNotFoundError(
                "Bucket file not found: "
                f"{source_path}"
            )

        results.append(
            process_bucket(
                canonical=canonical,
                bucket=bucket,
                source_path=source_path,
                source_file_order=(
                    source_file_order
                ),
                output_dir=output_dir,
                chunk_events=(
                    args.chunk_events
                ),
                progress_every=(
                    args.progress_every
                ),
                keep_temp=args.keep_temp,
            )
        )

    elapsed = (
        time.monotonic()
        - started
    )

    manifest = build_manifest(
        input_dir=input_dir,
        output_dir=output_dir,
        canonical_script=(
            canonical_script
        ),
        partition_manifest=(
            partition_manifest
        ),
        bucket_stats=results,
        elapsed_seconds=elapsed,
        chunk_events=(
            args.chunk_events
        ),
    )

    manifest_path = write_manifest(
        output_dir,
        manifest,
    )

    if not all(
        manifest["validation"].values()
    ):
        raise RuntimeError(
            "Extraction validation failed; "
            f"inspect {manifest_path}"
        )

    LOGGER.info(
    (
        "Extraction complete: %s input "
        "lines, %s retained events, "
        "%s buckets"
    ),
    f"{manifest['totals']['input_lines']:,}",
    f"{manifest['totals']['retained_events']:,}",
    f"{manifest['totals']['bucket_count']:,}",
    )

    print(
        json.dumps(
            {
                "status": "complete",
                "manifest": str(
                    manifest_path
                ),
                "input_lines": (
                    manifest["totals"][
                        "input_lines"
                    ]
                ),
                "retained_events": (
                    manifest["totals"][
                        "retained_events"
                    ]
                ),
                "bucket_count": (
                    manifest["totals"][
                        "bucket_count"
                    ]
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )

    return 0


def run_self_test(
    canonical_script: Path,
) -> int:
    canonical = load_canonical_module(
        canonical_script
    )

    with tempfile.TemporaryDirectory() as (
        temporary_text
    ):
        root = Path(temporary_text)

        raw_dir = root / "raw"
        raw_month = raw_dir / "2025"

        raw_month.mkdir(
            parents=True
        )

        source = (
            raw_month
            / "2025-01.log"
        )

        lines = [
            (
                "example.org 192.0.2.1 - "
                "user-1 "
                "[01/Jan/2025:12:00:05 "
                '+0000] "GET '
                "/download/json/token-1 "
                'HTTP/1.1" 200 100 "-" '
                '"agent" "-" 0.1\n'
            ),
            (
                "example.org 192.0.2.1 - "
                "user-1 "
                "[01/Jan/2025:12:00:00 "
                '+0000] "GET '
                "/download/json/prepare"
                "?token=token-1 "
                'HTTP/1.1" 200 90 "-" '
                '"agent" "-" 0.1\n'
            ),
            (
                "example.org 192.0.2.1 - - "
                "[01/Jan/2025:13:00:00 "
                '+0000] "GET '
                "/browse/processes/"
                "process-123 "
                'HTTP/1.1" 206 120 "-" '
                '"agent" "-" 0.1\n'
            ),
            (
                "example.org 192.0.2.1 - - "
                "[01/Jan/2025:14:00:00 "
                '+0000] "GET '
                "/repository/file/documents/"
                "example.pdf "
                'HTTP/1.1" 200 130 "-" '
                '"agent" "-" 0.1\n'
            ),
            (
                "example.org 192.0.2.1 - - "
                "[01/Jan/2025:15:00:00 "
                '+0000] "GET '
                "/download/json/"
                "repository_NREL@USLCI"
                "@revision-1 "
                'HTTP/1.1" 200 140 '
                '"https://www.lcacommons.gov/'
                "lca-collaboration/NREL/"
                'USLCI/datasets" '
                '"agent" "-" 0.1\n'
            ),
            (
                "example.org 192.0.2.1 - - "
                "[01/Jan/2025:16:00:00 "
                '+0000] "GET /health '
                'HTTP/1.1" 200 10 "-" '
                '"agent" "-" 0.1\n'
            ),
            "malformed line\n",
            "\n",
        ]

        source.write_text(
            "".join(lines),
            encoding="utf-8",
        )

        partition_manifest = {
            "totals": {
                "bucketed_lines": len(
                    lines
                ),
                "monthly_bucket_bytes": (
                    source.stat().st_size
                ),
            },
            "buckets": [
                {
                    "bucket": "2025-01",
                    "output_file": (
                        "2025/2025-01.log"
                    ),
                }
            ],
        }

        (
            raw_dir / "manifest.json"
        ).write_text(
            json.dumps(
                partition_manifest
            ),
            encoding="utf-8",
        )

        output_dir = root / "filtered"

        stats = process_bucket(
            canonical=canonical,
            bucket="2025-01",
            source_path=source,
            source_file_order=0,
            output_dir=output_dir,
            chunk_events=2,
            progress_every=0,
            keep_temp=False,
        )

        manifest = build_manifest(
            input_dir=raw_dir,
            output_dir=output_dir,
            canonical_script=(
                canonical_script
            ),
            partition_manifest=(
                partition_manifest
            ),
            bucket_stats=[stats],
            elapsed_seconds=0.0,
            chunk_events=2,
        )

        output_path = (
            output_dir
            / "2025"
            / "2025-01.jsonl"
        )

        documents = [
            json.loads(line)
            for line
            in output_path.read_text(
                encoding="utf-8"
            ).splitlines()
        ]

        timestamps = [
            item["timestamp"]
            for item in documents
        ]

        event_types = [
            item["event_type"]
            for item in documents
        ]

        checks = [
            (
                stats.input_lines == 8,
                (
                    "expected 8 lines, got "
                    f"{stats.input_lines}"
                ),
            ),
            (
                stats.blank_lines == 1,
                (
                    "expected 1 blank, got "
                    f"{stats.blank_lines}"
                ),
            ),
            (
                stats.malformed_lines == 1,
                (
                    "expected 1 malformed, got "
                    f"{stats.malformed_lines}"
                ),
            ),
            (
                stats.retained_events == 5,
                (
                    "expected 5 retained, got "
                    f"{stats.retained_events}"
                ),
            ),
            (
                stats.irrelevant_events == 1,
                (
                    "expected 1 irrelevant, got "
                    f"{stats.irrelevant_events}"
                ),
            ),
            (
                (
                    stats
                    .excluded_repository_exports
                    == 0
                ),
                (
                    "expected 0 excluded "
                    "repository exports"
                ),
            ),
            (
                (
                    stats
                    .filtered_input_out_of_order
                    == 1
                ),
                (
                    "expected 1 filtered "
                    "reversal"
                ),
            ),
            (
                stats.chunk_count == 3,
                (
                    "expected 3 chunks, got "
                    f"{stats.chunk_count}"
                ),
            ),
            (
                timestamps
                == sorted(timestamps),
                (
                    "output timestamps are "
                    "not sorted"
                ),
            ),
            (
                event_types
                == [
                    canonical.PREPARE,
                    (
                        canonical
                        .TOKEN_RETRIEVAL
                    ),
                    (
                        canonical
                        .BROWSE_DATASET
                    ),
                    (
                        canonical
                        .REPOSITORY_FILE
                    ),
                    REPOSITORY_EXPORT,
                ],
                (
                    "unexpected event types: "
                    f"{event_types}"
                ),
            ),
            (
                documents[-1].get("repository_key")
                == "NREL/USLCI",
                (
                    "unexpected repository key: "
                    + str(
                        documents[-1].get(
                            "repository_key"
                        )
                    )
                ),
            ),
            (
                documents[-1].get("resource_id")
                == "revision-1",
                (
                    "unexpected export resource id: "
                    + str(
                        documents[-1].get(
                            "resource_id"
                        )
                    )
                ),
            ),
            (
                all(
                    manifest[
                        "validation"
                    ].values()
                ),
                (
                    "manifest validation "
                    "failed"
                ),
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
                    (
                        "SELF-TEST FAILED: "
                        f"{failure}"
                    ),
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

    configure_logging(
        args.verbose
    )

    try:
        if args.self_test:
            return run_self_test(
                Path(
                    args.canonical_script
                )
            )

        return run_pipeline(args)

    except (
        FileNotFoundError,
        ImportError,
        OSError,
        ValueError,
        RuntimeError,
    ) as exc:
        LOGGER.error(
            "%s",
            exc,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())