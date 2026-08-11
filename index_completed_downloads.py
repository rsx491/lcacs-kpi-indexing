#!/usr/bin/env python3
"""
index_completed_downloads.py

Canonical FLCAC Completed Download KPI
======================================

Business definition
-------------------

A completed download is one confirmed instance of a user or client
successfully retrieving requested data from FLCAC, counted once per
transaction regardless of how many technical API calls were required.

A completed download is created from one of the following:

1. Token-based repository/package download

   A successful:

       /download/json/{token}

   request matched to its corresponding prepare request.

   The prepare and retrieval pair counts as one completed download.

2. Single-dataset download

   A successful qualifying:

       /browse/...

   request counts as one completed download.

3. Single-file download

   A successful:

       /repository/file/...

   request counts as one completed download.

Client identity priority
------------------------

1. Authenticated user
2. API key
3. Session identifier
4. IP address

Retry deduplication
-------------------

Requests are considered retries when all of the following match:

- client identifier
- repository
- resource type
- resource identifier
- download type

and the requests occur within the configured retry window, which
defaults to 60 seconds.

Excluded from the official KPI
------------------------------

- Prepare-only requests
- Failed retrievals
- Unmatched token retrievals
- Duplicate/retry requests
- Requests lacking sufficient repository/resource identity

Important token-correlation note
--------------------------------

A standard Apache access log normally records the prepare request but
does not record the token returned in the HTTP response body.

Exact prepare/retrieval correlation therefore requires at least one of:

- enriched JSON logs containing the prepare response token;
- the token appearing in the prepare request or query string;
- a separate token mapping file;
- application logs containing the token and prepare metadata.

This script supports all three practical inputs:

- token directly present in an enriched JSON event;
- token present in the request URL/query string;
- an optional CSV token mapping supplied with --prepare-token-map.

Python requirement
------------------

Python 3.10 or newer is recommended.

No third-party Python packages are required.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import ipaddress
import json
import logging
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence


LOGGER = logging.getLogger("completed-downloads")


DEFAULT_DEDUPE_SECONDS = 60
DEFAULT_REQUEST_TIMEOUT = 60


PREPARE = "prepare"
TOKEN_RETRIEVAL = "token_retrieval"
REPOSITORY_EXPORT = "repository_export"
BROWSE_DATASET = "browse_dataset"
REPOSITORY_FILE = "repository_file"

SUPPORTED_DOWNLOAD_TYPES = {
    TOKEN_RETRIEVAL,
    REPOSITORY_EXPORT,
    BROWSE_DATASET,
    REPOSITORY_FILE,
}


# Apache Combined Log Format:
#
# 127.0.0.1 - user [10/Oct/2000:13:55:36 -0700]
# "GET /path HTTP/1.1" 200 1234
# "https://referer" "user-agent"
#
APACHE_LOG_PATTERN = re.compile(
    r'^(?P<server_name>\S+)\s+'
    r'(?P<ip>\S+)\s+'
    r'(?P<ident>\S+)\s+'
    r'(?P<auth_user>\S+)\s+'
    r'\[(?P<timestamp>[^\]]+)\]\s+'
    r'"(?P<method>\S+)\s+(?P<request>\S+)(?:\s+HTTP/(?P<http_version>[^"]+))?"\s+'
    r'(?P<status>\d{3}|-)\s+'
    r'(?P<size>\S+)\s+'
    r'"(?P<referer>[^"]*)"\s+'
    r'"(?P<user_agent>[^"]*)"\s+'
    r'"(?P<client_endpoint>[^"]*)"\s+'
    r'(?P<request_time>\S+)$'
)


TOKEN_RETRIEVAL_PATTERN = re.compile(
    r"/download/json/(?P<token>[^/?#]+)(?:[/?#]|$)",
    re.IGNORECASE,
)

PREPARE_PATTERN = re.compile(
    r"/(?:download/json/)?prepare(?:[/?#]|$)",
    re.IGNORECASE,
)

REPOSITORY_FILE_PATTERN = re.compile(
    r"/repository/file(?:[/?#]|$)",
    re.IGNORECASE,
)

BROWSE_PATTERN = re.compile(
    r"/browse(?:[/?#]|$)",
    re.IGNORECASE,
)

PUBLIC_BROWSE_DATASET_PATTERN = re.compile(
    r"^/lca-collaboration/ws/public/browse/"
    r"(?P<group>[^/]+)/"
    r"(?P<repo>[^/]+)/"
    r"(?P<resource_type>[^/]+)/"
    r"(?P<resource_id>[^/]+)$",
re.IGNORECASE,
)

PUBLIC_REPOSITORY_FILE_PATTERN = re.compile(
    r"^/lca-collaboration/ws/public/repository/file/"
    r"(?P<group>[^/]+)/"
    r"(?P<repo>[^/]+)/"
    r"(?P<resource_type>[^/]+)/"
    r"(?P<resource_id>[^/]+)/"
    r"(?P<file_path>.+)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ParsedEvent:
    timestamp: datetime
    method: str
    request: str
    path: str
    query: dict[str, list[str]]
    status: int
    client_ip: str
    event_type: str | None = None

    user_id: str | None = None
    api_key: str | None = None
    session_id: str | None = None

    repository: str | None = None
    resource_type: str | None = None
    resource_id: str | None = None

    token: str | None = None
    prepare_token: str | None = None

    bytes_sent: int | None = None
    user_agent: str | None = None
    referer: str | None = None

    source_file: str | None = None
    source_line: int | None = None


@dataclass(frozen=True)
class ClientIdentity:
    identity_type: str
    identity_value: str


@dataclass(frozen=True)
class CompletedDownload:
    timestamp: datetime
    event_type: str
    download_type: str

    repository: str
    resource_type: str
    resource_id: str

    client_identifier_type: str
    client_identifier: str

    retrieval_path: str
    retrieval_status: int

    prepare_timestamp: datetime | None = None
    token: str | None = None

    source_file: str | None = None
    source_line: int | None = None


@dataclass
class Metrics:
    total_lines: int = 0
    parsed_events: int = 0
    malformed_lines: int = 0
    outside_date_range: int = 0
    irrelevant_events: int = 0

    prepare_requests: int = 0
    successful_prepare_requests: int = 0
    prepare_requests_with_token: int = 0

    token_retrieval_requests: int = 0
    successful_token_retrievals: int = 0
    matched_token_retrievals: int = 0
    unmatched_token_retrievals: int = 0

    browse_requests: int = 0
    successful_browse_downloads: int = 0

    repository_file_requests: int = 0
    successful_repository_file_downloads: int = 0

    failed_retrievals: int = 0
    missing_resource_identity: int = 0
    duplicate_retries: int = 0
    abandoned_prepares: int = 0

    completed_downloads: int = 0

    completed_by_type: dict[str, int] = field(default_factory=dict)
    completed_by_repository: dict[str, int] = field(default_factory=dict)


@dataclass
class ProcessingResult:
    downloads: list[CompletedDownload]
    metrics: Metrics


@dataclass(frozen=True)
class TokenMapEntry:
    token: str
    timestamp: datetime | None
    repository: str | None
    resource_type: str | None
    resource_id: str | None
    client_identifier_type: str | None
    client_identifier: str | None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build the canonical completed-download KPI from FLCAC access logs."
        )
    )

    parser.add_argument(
        "logs",
        nargs="*",
        help="Apache access-log or JSONL files to process.",
    )

    parser.add_argument(
        "--start-date",
        help="Inclusive start date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--end-date",
        help=(
            "Exclusive end date in YYYY-MM-DD format. "
            "For FY2025, use 2025-10-01."
        ),
    )

    parser.add_argument(
        "--dedupe-window-seconds",
        type=int,
        default=DEFAULT_DEDUPE_SECONDS,
        help="Retry deduplication window. Default: 60 seconds.",
    )

    parser.add_argument(
        "--prepare-token-map",
        help=(
            "Optional CSV containing prepare token mappings. Expected columns: "
            "token,timestamp,repository,resource_type,resource_id,"
            "client_identifier_type,client_identifier"
        ),
    )

    parser.add_argument(
        "--output-jsonl",
        help="Write one completed-download document per line.",
    )

    parser.add_argument(
        "--summary-json",
        help="Write processing metrics as JSON.",
    )

    parser.add_argument(
        "--es-url",
        help="Optional Elasticsearch/OpenSearch URL, such as http://localhost:9200.",
    )

    parser.add_argument(
        "--index",
        help="Target Elasticsearch/OpenSearch index name.",
    )

    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Delete and recreate the target index before indexing.",
    )

    parser.add_argument(
        "--raw-client-identifiers",
        action="store_true",
        help=(
            "Store raw client identifiers. By default, identifiers are hashed "
            "before output or indexing."
        ),
    )

    parser.add_argument(
        "--client-hash-salt",
        default="",
        help="Optional salt used when hashing client identifiers.",
    )

    parser.add_argument(
        "--allow-unmatched-token-retrievals",
        action="store_true",
        help=(
            "Count successful token retrievals without a matched prepare. "
            "This is disabled by default because it does not satisfy the "
            "agreed business definition."
        ),
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run the built-in synthetic test instead of reading log files.",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )

    args = parser.parse_args(argv)

    if not args.self_test and not args.logs:
        parser.error("Provide at least one log file or use --self-test.")

    if bool(args.es_url) != bool(args.index):
        parser.error("--es-url and --index must be provided together.")

    if args.dedupe_window_seconds < 0:
        parser.error("--dedupe-window-seconds cannot be negative.")

    return args


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    text = value.strip()

    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def parse_apache_datetime(value: str) -> datetime:
    return datetime.strptime(
        value,
        "%d/%b/%Y:%H:%M:%S %z",
    ).astimezone(timezone.utc)


def parse_date_boundary(
    value: str | None,
    *,
    name: str,
) -> datetime | None:
    if value is None:
        return None

    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"{name} must use YYYY-MM-DD format: {value!r}"
        ) from exc

    return datetime(
        parsed.year,
        parsed.month,
        parsed.day,
        tzinfo=timezone.utc,
    )


def first_nonempty(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue

        text = str(value).strip()

        if text and text != "-":
            return text

    return None


def first_query_value(
    query: dict[str, list[str]],
    *names: str,
) -> str | None:
    lowered = {
        key.lower(): values
        for key, values in query.items()
    }

    for name in names:
        values = lowered.get(name.lower())

        if values:
            value = first_nonempty(*values)

            if value:
                return value

    return None


def normalize_status(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def normalize_bytes(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_success(status: int) -> bool:
    return 200 <= status < 300


def is_valid_ip(value: str | None) -> bool:
    if not value:
        return False

    candidate = value.split(",")[0].strip()

    try:
        ipaddress.ip_address(candidate)
    except ValueError:
        return False

    return True


def normalize_ip(value: str | None) -> str:
    if not value:
        return "unknown"

    candidate = value.split(",")[0].strip()

    return candidate if is_valid_ip(candidate) else value.strip()


def flatten_json_headers(data: dict[str, Any]) -> dict[str, str]:
    headers = data.get("headers")

    if not isinstance(headers, dict):
        return {}

    return {
        str(key).lower(): str(value)
        for key, value in headers.items()
        if value is not None
    }


def parse_request_url(request: str) -> tuple[str, dict[str, list[str]]]:
    request = request.strip()

    parsed = urllib.parse.urlsplit(request)

    path = parsed.path or "/"
    query = urllib.parse.parse_qs(
        parsed.query,
        keep_blank_values=True,
    )

    return path, query


def extract_token_from_retrieval_path(path: str) -> str | None:
    match = TOKEN_RETRIEVAL_PATTERN.search(path)

    if not match:
        return None

    token = urllib.parse.unquote(match.group("token")).strip()

    if token.lower() in {"prepare", "ws", "public"}:
        return None

    return token or None


def extract_prepare_token(
    data: dict[str, Any],
    query: dict[str, list[str]],
) -> str | None:
    return first_nonempty(
        data.get("prepare_token"),
        data.get("response_token"),
        data.get("download_token"),
        data.get("token"),
        first_query_value(
            query,
            "prepare_token",
            "response_token",
            "download_token",
            "token",
        ),
    )


def extract_repository(
    data: dict[str, Any],
    query: dict[str, list[str]],
    path: str,
) -> str | None:
    explicit = first_nonempty(
        data.get("repository_key"),
        data.get("repository"),
        data.get("repository_id"),
        data.get("repo"),
        first_query_value(
            query,
            "repository",
            "repository_id",
            "repo",
            "repositoryId",
        ),
    )

    if explicit:
        return explicit

    for route_pattern in (
        PUBLIC_BROWSE_DATASET_PATTERN,
        PUBLIC_REPOSITORY_FILE_PATTERN,
    ):
        match = route_pattern.fullmatch(path)

        if match:
            group = urllib.parse.unquote(match.group("group"))
            repo = urllib.parse.unquote(match.group("repo"))
            return f"{group}/{repo}"

    patterns = (
        r"/repositories/(?P<value>[^/?#]+)",
        r"/repository/(?P<value>[^/?#]+)",
        r"/repos/(?P<value>[^/?#]+)",
    )

    for pattern in patterns:
        match = re.search(pattern, path, re.IGNORECASE)

        if match:
            return urllib.parse.unquote(match.group("value"))

    return None


def extract_resource_type(
    data: dict[str, Any],
    query: dict[str, list[str]],
    path: str,
    event_type: str | None,
) -> str | None:
    explicit = first_nonempty(
        data.get("resource_type"),
        data.get("dataset_type"),
        data.get("type"),
        first_query_value(
            query,
            "resource_type",
            "dataset_type",
            "type",
            "modelType",
        ),
    )

    if explicit:
        return explicit

    for route_pattern in (
        PUBLIC_BROWSE_DATASET_PATTERN,
        PUBLIC_REPOSITORY_FILE_PATTERN,
    ):
        match = route_pattern.fullmatch(path)

        if match:
            return urllib.parse.unquote(
                match.group("resource_type")
            ).upper()

    lowered_path = path.lower()

    known_types = (
        "processes",
        "process",
        "flows",
        "flow",
        "flow-properties",
        "flowproperty",
        "unit-groups",
        "unitgroup",
        "actors",
        "actor",
        "sources",
        "source",
        "categories",
        "category",
    )

    for known_type in known_types:
        if f"/{known_type}/" in lowered_path:
            return known_type.rstrip("s")

    if event_type == REPOSITORY_FILE:
        return "file"

    if event_type == TOKEN_RETRIEVAL:
        return "prepared_download"

    if event_type == BROWSE_DATASET:
        return "dataset"

    return None


def extract_resource_id(
    data: dict[str, Any],
    query: dict[str, list[str]],
    path: str,
    event_type: str | None,
) -> str | None:
    explicit = first_nonempty(
        data.get("resource_id"),
        data.get("dataset_id"),
        data.get("file_id"),
        data.get("uuid"),
        data.get("id"),
        data.get("file_path"),
        first_query_value(
            query,
            "resource_id",
            "dataset_id",
            "file_id",
            "uuid",
            "id",
            "refId",
            "file",
            "path",
        ),
    )

    if explicit:
        return explicit

    for route_pattern in (
        PUBLIC_BROWSE_DATASET_PATTERN,
        PUBLIC_REPOSITORY_FILE_PATTERN,
    ):
        match = route_pattern.fullmatch(path)

        if match:
            return urllib.parse.unquote(
                match.group("resource_id")
            )

    if event_type == REPOSITORY_FILE:
        match = re.search(
            r"/repository/file/(.+)$",
            path,
            re.IGNORECASE,
        )

        if match:
            return urllib.parse.unquote(
                match.group(1)
            ).strip("/")

    if event_type == BROWSE_DATASET:
        segments = [
            urllib.parse.unquote(segment)
            for segment in path.split("/")
            if segment
        ]

        try:
            browse_index = [
                segment.lower()
                for segment in segments
            ].index("browse")
        except ValueError:
            browse_index = -1

        if browse_index >= 0:
            trailing = segments[browse_index + 1 :]

            if trailing:
                return trailing[-1]

    return None


def classify_event(path: str) -> str | None:
    if PREPARE_PATTERN.search(path):
        return PREPARE

    if TOKEN_RETRIEVAL_PATTERN.search(path):
        return TOKEN_RETRIEVAL

    if REPOSITORY_FILE_PATTERN.search(path):
        return REPOSITORY_FILE

    if BROWSE_PATTERN.search(path):
        return BROWSE_DATASET

    return None


def qualifies_as_single_dataset_download(event: ParsedEvent) -> bool:
    """
    Count only exact individual-dataset browse requests:

    /lca-collaboration/ws/public/browse/
    {group}/{repo}/{type}/{refId}
    """
    return bool(
        PUBLIC_BROWSE_DATASET_PATTERN.fullmatch(event.path)
)


def event_from_json(
    data: dict[str, Any],
    *,
    source_file: str,
    source_line: int,
) -> ParsedEvent:
    headers = flatten_json_headers(data)

    request = first_nonempty(
        data.get("request"),
        data.get("request_uri"),
        data.get("uri"),
        data.get("url"),
        data.get("path"),
    ) or "/"

    path, query = parse_request_url(request)
    event_type = first_nonempty(
    data.get("event_type"),
    classify_event(path),
)

    timestamp = parse_iso_datetime(
        first_nonempty(
            data.get("@timestamp"),
            data.get("timestamp"),
            data.get("time"),
            data.get("datetime"),
        )
    )

    if timestamp is None:
        raise ValueError("JSON event does not contain a valid timestamp")

    client_ip = normalize_ip(
        first_nonempty(
            data.get("client_ip"),
            data.get("remote_addr"),
            data.get("ip"),
            headers.get("x-forwarded-for"),
        )
    )

    user_id = first_nonempty(
        data.get("user_id"),
        data.get("username"),
        data.get("authenticated_user"),
        data.get("remote_user"),
        headers.get("x-user-id"),
    )

    api_key = first_nonempty(
        data.get("api_key"),
        data.get("api_key_id"),
        headers.get("x-api-key"),
        headers.get("authorization"),
    )

    session_id = first_nonempty(
        data.get("session_id"),
        data.get("session"),
        headers.get("x-session-id"),
        first_query_value(query, "session", "session_id"),
    )

    token = first_nonempty(
        data.get("retrieval_token"),
        extract_token_from_retrieval_path(path),
    )

    prepare_token = (
        extract_prepare_token(data, query)
        if event_type == PREPARE
        else None
    )

    repository = extract_repository(data, query, path)
    resource_type = extract_resource_type(
        data,
        query,
        path,
        event_type,
    )
    resource_id = extract_resource_id(
        data,
        query,
        path,
        event_type,
    )

    return ParsedEvent(
        timestamp=timestamp,
        method=first_nonempty(
            data.get("method"),
            data.get("http_method"),
        ) or "GET",
        request=request,
        path=path,
        query=query,
        status=normalize_status(
            first_nonempty(
                data.get("status"),
                data.get("status_code"),
                data.get("response_status"),
            )
        ),
        client_ip=client_ip,
        event_type=event_type,
        user_id=user_id,
        api_key=api_key,
        session_id=session_id,
        repository=repository,
        resource_type=resource_type,
        resource_id=resource_id,
        token=token,
        prepare_token=prepare_token,
        bytes_sent=normalize_bytes(
            first_nonempty(
                data.get("bytes_sent"),
                data.get("response_bytes"),
                data.get("size"),
            )
        ),
        user_agent=first_nonempty(
            data.get("user_agent"),
            headers.get("user-agent"),
        ),
        referer=first_nonempty(
            data.get("referer"),
            headers.get("referer"),
        ),
        source_file=source_file,
        source_line=source_line,
    )


def event_from_apache(
    line: str,
    *,
    source_file: str,
    source_line: int,
) -> ParsedEvent:
    match = APACHE_LOG_PATTERN.match(line)

    if not match:
        raise ValueError("Line does not match Apache access-log format")

    values = match.groupdict()

    request = values.get("request") or "/"
    path, query = parse_request_url(request)
    event_type = classify_event(path)

    auth_user = first_nonempty(values.get("auth_user"))

    token = extract_token_from_retrieval_path(path)
    prepare_token = (
        first_query_value(
            query,
            "prepare_token",
            "response_token",
            "download_token",
            "token",
        )
        if event_type == PREPARE
        else None
    )

    empty_data: dict[str, Any] = {}

    return ParsedEvent(
        timestamp=parse_apache_datetime(values["timestamp"]),
        method=values.get("method") or "GET",
        request=request,
        path=path,
        query=query,
        status=normalize_status(values.get("status")),
        client_ip=normalize_ip(values.get("ip")),
        user_id=auth_user,
        api_key=first_query_value(
            query,
            "api_key",
            "apikey",
            "key",
        ),
        session_id=first_query_value(
            query,
            "session",
            "session_id",
        ),
        repository=extract_repository(
            empty_data,
            query,
            path,
        ),
        resource_type=extract_resource_type(
            empty_data,
            query,
            path,
            event_type,
        ),
        resource_id=extract_resource_id(
            empty_data,
            query,
            path,
            event_type,
        ),
        token=token,
        prepare_token=prepare_token,
        bytes_sent=normalize_bytes(values.get("size")),
        user_agent=first_nonempty(values.get("user_agent")),
        referer=first_nonempty(values.get("referer")),
        source_file=source_file,
        source_line=source_line,
    )


def parse_log_line(
    line: str,
    *,
    source_file: str,
    source_line: int,
) -> ParsedEvent:
    stripped = line.strip()

    if not stripped:
        raise ValueError("Empty line")

    if stripped.startswith("{"):
        value = json.loads(stripped)

        if not isinstance(value, dict):
            raise ValueError("JSON log line must be an object")

        return event_from_json(
            value,
            source_file=source_file,
            source_line=source_line,
        )

    return event_from_apache(
        stripped,
        source_file=source_file,
        source_line=source_line,
    )


def iter_log_events(
    paths: Sequence[str],
    metrics: Metrics,
) -> Iterator[ParsedEvent]:
    for path_text in paths:
        path = Path(path_text)

        if not path.is_file():
            raise FileNotFoundError(f"Log file not found: {path}")

        LOGGER.info("Reading %s", path)

        with path.open(
            "r",
            encoding="utf-8",
            errors="replace",
        ) as handle:
            for line_number, line in enumerate(handle, start=1):
                metrics.total_lines += 1

                if not line.strip():
                    continue

                try:
                    event = parse_log_line(
                        line,
                        source_file=str(path),
                        source_line=line_number,
                    )
                except (ValueError, json.JSONDecodeError) as exc:
                    metrics.malformed_lines += 1
                    LOGGER.debug(
                        "Skipping malformed line %s:%d: %s",
                        path,
                        line_number,
                        exc,
                    )
                    continue

                metrics.parsed_events += 1
                yield event


def resolve_client(event: ParsedEvent) -> ClientIdentity:
    if event.user_id:
        return ClientIdentity("user", event.user_id)

    if event.api_key:
        return ClientIdentity("api_key", event.api_key)

    if event.session_id:
        return ClientIdentity("session", event.session_id)

    return ClientIdentity("ip", event.client_ip or "unknown")


def protected_client_value(
    identity: ClientIdentity,
    *,
    preserve_raw: bool,
    salt: str,
) -> str:
    if preserve_raw:
        return identity.identity_value

    digest = hashlib.sha256(
        f"{salt}|{identity.identity_type}|{identity.identity_value}".encode(
            "utf-8"
        )
    ).hexdigest()

    return digest


def in_date_range(
    timestamp: datetime,
    start: datetime | None,
    end: datetime | None,
) -> bool:
    if start is not None and timestamp < start:
        return False

    if end is not None and timestamp >= end:
        return False

    return True


def load_token_map(
    path_text: str | None,
) -> dict[str, TokenMapEntry]:
    if not path_text:
        return {}

    path = Path(path_text)

    if not path.is_file():
        raise FileNotFoundError(f"Token map not found: {path}")

    entries: dict[str, TokenMapEntry] = {}

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames or "token" not in reader.fieldnames:
            raise ValueError(
                "Prepare token map must contain a 'token' column"
            )

        for row_number, row in enumerate(reader, start=2):
            token = first_nonempty(row.get("token"))

            if not token:
                LOGGER.warning(
                    "Skipping token-map row %d with no token",
                    row_number,
                )
                continue

            entries[token] = TokenMapEntry(
                token=token,
                timestamp=parse_iso_datetime(row.get("timestamp")),
                repository=first_nonempty(row.get("repository")),
                resource_type=first_nonempty(
                    row.get("resource_type")
                ),
                resource_id=first_nonempty(row.get("resource_id")),
                client_identifier_type=first_nonempty(
                    row.get("client_identifier_type")
                ),
                client_identifier=first_nonempty(
                    row.get("client_identifier")
                ),
            )

    LOGGER.info(
        "Loaded %,d prepare-token mappings from %s",
        len(entries),
        path,
    )

    return entries


def build_download_from_single_call(
    event: ParsedEvent,
    *,
    download_type: str,
    preserve_raw_clients: bool,
    client_hash_salt: str,
) -> CompletedDownload | None:
    if not (
        event.repository
        and event.resource_type
        and event.resource_id
    ):
        return None

    client = resolve_client(event)

    return CompletedDownload(
        timestamp=event.timestamp,
        event_type="completed_download",
        download_type=download_type,
        repository=event.repository,
        resource_type=event.resource_type,
        resource_id=event.resource_id,
        client_identifier_type=client.identity_type,
        client_identifier=protected_client_value(
            client,
            preserve_raw=preserve_raw_clients,
            salt=client_hash_salt,
        ),
        retrieval_path=event.path,
        retrieval_status=event.status,
        source_file=event.source_file,
        source_line=event.source_line,
    )


def correlate_token_retrieval(
    retrieval: ParsedEvent,
    prepare_by_token: dict[str, ParsedEvent],
    external_token_map: dict[str, TokenMapEntry],
    *,
    preserve_raw_clients: bool,
    client_hash_salt: str,
    allow_unmatched: bool,
) -> CompletedDownload | None:
    if not retrieval.token:
        return None

    prepare = prepare_by_token.get(retrieval.token)
    external = external_token_map.get(retrieval.token)

    if prepare is None and external is None and not allow_unmatched:
        return None

    repository = first_nonempty(
        retrieval.repository,
        prepare.repository if prepare else None,
        external.repository if external else None,
    )

    resource_type = first_nonempty(
        retrieval.resource_type,
        prepare.resource_type if prepare else None,
        external.resource_type if external else None,
    )

    resource_id = first_nonempty(
        retrieval.resource_id,
        prepare.resource_id if prepare else None,
        external.resource_id if external else None,
    )

    if allow_unmatched:
        repository = repository or "unknown"
        resource_type = resource_type or "prepared_download"
        resource_id = resource_id or retrieval.token

    if not repository or not resource_type or not resource_id:
        return None

    retrieval_client = resolve_client(retrieval)

    if (
        retrieval_client.identity_type == "ip"
        and prepare is not None
    ):
        prepare_client = resolve_client(prepare)

        if prepare_client.identity_type != "ip":
            retrieval_client = prepare_client

    if (
        retrieval_client.identity_type == "ip"
        and external
        and external.client_identifier_type
        and external.client_identifier
    ):
        retrieval_client = ClientIdentity(
            external.client_identifier_type,
            external.client_identifier,
        )

    prepare_timestamp = first_nonempty(
        prepare.timestamp.isoformat() if prepare else None,
        external.timestamp.isoformat()
        if external and external.timestamp
        else None,
    )

    parsed_prepare_timestamp = (
        parse_iso_datetime(prepare_timestamp)
        if prepare_timestamp
        else None
    )

    return CompletedDownload(
        timestamp=retrieval.timestamp,
        event_type="completed_download",
        download_type=TOKEN_RETRIEVAL,
        repository=repository,
        resource_type=resource_type,
        resource_id=resource_id,
        client_identifier_type=retrieval_client.identity_type,
        client_identifier=protected_client_value(
            retrieval_client,
            preserve_raw=preserve_raw_clients,
            salt=client_hash_salt,
        ),
        retrieval_path=retrieval.path,
        retrieval_status=retrieval.status,
        prepare_timestamp=parsed_prepare_timestamp,
        token=retrieval.token,
        source_file=retrieval.source_file,
        source_line=retrieval.source_line,
    )


def dedupe_key(
    download: CompletedDownload,
) -> tuple[str, str, str, str, str, str]:
    return (
        download.client_identifier_type,
        download.client_identifier,
        download.repository,
        download.resource_type,
        download.resource_id,
        download.download_type,
    )


def deduplicate_downloads(
    candidates: Iterable[CompletedDownload],
    *,
    window_seconds: int,
) -> tuple[list[CompletedDownload], int]:
    candidates_list = list(candidates)

    if window_seconds <= 0:
        return candidates_list, 0

    window = timedelta(seconds=window_seconds)

    completed: list[CompletedDownload] = []
    last_counted_by_key: dict[
        tuple[str, str, str, str, str, str],
        datetime,
    ] = {}

    duplicate_count = 0

    for candidate in sorted(
        candidates_list,
        key=lambda item: item.timestamp,
    ):
        key = dedupe_key(candidate)
        last_counted = last_counted_by_key.get(key)

        if (
            last_counted is not None
            and candidate.timestamp - last_counted <= window
        ):
            duplicate_count += 1
            continue

        completed.append(candidate)
        last_counted_by_key[key] = candidate.timestamp

    return completed, duplicate_count


def process_events(
    events: Iterable[ParsedEvent],
    *,
    start: datetime | None,
    end: datetime | None,
    dedupe_window_seconds: int,
    external_token_map: dict[str, TokenMapEntry],
    preserve_raw_clients: bool,
    client_hash_salt: str,
    allow_unmatched_token_retrievals: bool,
    metrics: Metrics | None = None,
) -> ProcessingResult:
    metrics = metrics or Metrics()

    prepare_by_token: dict[str, ParsedEvent] = {}
    prepare_tokens_seen: set[str] = set()
    matched_prepare_tokens: set[str] = set()

    candidates: list[CompletedDownload] = []

    sorted_events = sorted(
        events,
        key=lambda item: item.timestamp,
    )

    for event in sorted_events:
        if not in_date_range(event.timestamp, start, end):
            metrics.outside_date_range += 1
            continue

        event_type = event.event_type or classify_event(event.path)

        if event_type is None:
            metrics.irrelevant_events += 1
            continue

        if event_type == PREPARE:
            metrics.prepare_requests += 1

            if not is_success(event.status):
                continue

            metrics.successful_prepare_requests += 1

            if event.prepare_token:
                metrics.prepare_requests_with_token += 1
                prepare_by_token[event.prepare_token] = event
                prepare_tokens_seen.add(event.prepare_token)

            continue

        if event_type == REPOSITORY_EXPORT:
            metrics.token_retrieval_requests += 1

            if event.status != 200:
                metrics.failed_retrievals += 1
                continue

            candidate = build_download_from_repository_export(
                event,
                preserve_raw_clients=preserve_raw_clients,
                client_hash_salt=client_hash_salt,
            )

            if candidate is None:
                metrics.missing_resource_identity += 1
                continue

            metrics.successful_token_retrievals += 1
            metrics.matched_token_retrievals += 1
            candidates.append(candidate)
            continue

        if event_type == TOKEN_RETRIEVAL:
            metrics.token_retrieval_requests += 1

            if not is_success(event.status):
                metrics.failed_retrievals += 1
                continue

            metrics.successful_token_retrievals += 1

            candidate = correlate_token_retrieval(
                event,
                prepare_by_token,
                external_token_map,
                preserve_raw_clients=preserve_raw_clients,
                client_hash_salt=client_hash_salt,
                allow_unmatched=allow_unmatched_token_retrievals,
            )

            if candidate is None:
                metrics.unmatched_token_retrievals += 1
                continue

            metrics.matched_token_retrievals += 1

            if event.token:
                matched_prepare_tokens.add(event.token)

            candidates.append(candidate)
            continue

        if event_type == BROWSE_DATASET:
            metrics.browse_requests += 1

            if not is_success(event.status):
                metrics.failed_retrievals += 1
                continue

            if not qualifies_as_single_dataset_download(event):
                metrics.missing_resource_identity += 1
                continue

            candidate = build_download_from_single_call(
                event,
                download_type=BROWSE_DATASET,
                preserve_raw_clients=preserve_raw_clients,
                client_hash_salt=client_hash_salt,
            )

            if candidate is None:
                metrics.missing_resource_identity += 1
                continue

            metrics.successful_browse_downloads += 1
            candidates.append(candidate)
            continue

        if event_type == REPOSITORY_FILE:
            metrics.repository_file_requests += 1

            if not is_success(event.status):
                metrics.failed_retrievals += 1
                continue

            candidate = build_download_from_single_call(
                event,
                download_type=REPOSITORY_FILE,
                preserve_raw_clients=preserve_raw_clients,
                client_hash_salt=client_hash_salt,
            )

            if candidate is None:
                metrics.missing_resource_identity += 1
                continue

            metrics.successful_repository_file_downloads += 1
            candidates.append(candidate)
            continue

    downloads, duplicate_count = deduplicate_downloads(
        candidates,
        window_seconds=dedupe_window_seconds,
    )

    public_repo_keys_path = Path("public_repo_keys.txt")

    public_repository_keys = {
        line.strip()
        for line in public_repo_keys_path.read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    }

    downloads = [
        download
        for download in downloads
        if download.repository in public_repository_keys
    ]

    metrics.duplicate_retries = duplicate_count
    metrics.abandoned_prepares = len(
        prepare_tokens_seen - matched_prepare_tokens
    )
    metrics.completed_downloads = len(downloads)

    for download in downloads:
        metrics.completed_by_type[download.download_type] = (
            metrics.completed_by_type.get(
                download.download_type,
                0,
            )
            + 1
        )

        metrics.completed_by_repository[download.repository] = (
            metrics.completed_by_repository.get(
                download.repository,
                0,
            )
            + 1
        )

    return ProcessingResult(
        downloads=downloads,
        metrics=metrics,
)

def build_download_from_repository_export(
        event: ParsedEvent,
        *,
        preserve_raw_clients: bool,
        client_hash_salt: str,
    ) -> CompletedDownload | None:
        """
        Build a completed-download document from an already-normalized
        repository_export JSONL event.
        """
        if not event.repository:
            return None

        client = resolve_client(event)

        return CompletedDownload(
            timestamp=event.timestamp,
            event_type="completed_download",
            download_type=REPOSITORY_EXPORT,
            repository=event.repository,
            resource_type=event.resource_type or "repository",
            resource_id=(
                event.resource_id
                or event.token
                or event.repository
            ),
            client_identifier_type=client.identity_type,
            client_identifier=protected_client_value(
                client,
                preserve_raw=preserve_raw_clients,
                salt=client_hash_salt,
            ),
            retrieval_path=event.path,
            retrieval_status=event.status,
            token=event.token,
            source_file=event.source_file,
            source_line=event.source_line,
        )

def download_to_document(
    download: CompletedDownload,
) -> dict[str, Any]:
    document = asdict(download)

    document["@timestamp"] = download.timestamp.isoformat()
    document.pop("timestamp", None)

    if download.prepare_timestamp is not None:
        document["prepare_timestamp"] = (
            download.prepare_timestamp.isoformat()
        )

    document["business_definition_version"] = "completed-download-v1"
    document["dedupe_rule"] = (
        "client+repository+resource_type+resource_id+download_type"
    )

    return {
        key: value
        for key, value in document.items()
        if value is not None
    }


def metrics_to_document(
    metrics: Metrics,
    *,
    start: datetime | None,
    end: datetime | None,
    dedupe_window_seconds: int,
) -> dict[str, Any]:
    value = asdict(metrics)

    value.update(
        {
            "document_type": "kpi_summary",
            "business_definition_version": "completed-download-v1",
            "period_start": start.isoformat() if start else None,
            "period_end_exclusive": end.isoformat() if end else None,
            "dedupe_window_seconds": dedupe_window_seconds,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    return {
        key: item
        for key, item in value.items()
        if item is not None
    }


def write_jsonl(
    path_text: str,
    downloads: Sequence[CompletedDownload],
) -> None:
    path = Path(path_text)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        for download in downloads:
            handle.write(
                json.dumps(
                    download_to_document(download),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            handle.write("\n")

    LOGGER.info(
        "Wrote %s completed downloads to %s",
        f"{len(downloads):,}",
        path_text,
    )


def write_summary_json(
    path_text: str,
    metrics: Metrics,
    *,
    start: datetime | None,
    end: datetime | None,
    dedupe_window_seconds: int,
) -> None:
    path = Path(path_text)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            metrics_to_document(
                metrics,
                start=start,
                end=end,
                dedupe_window_seconds=dedupe_window_seconds,
            ),
            handle,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")

    LOGGER.info("Wrote KPI summary to %s", path)


def http_request(
    method: str,
    url: str,
    *,
    body: bytes | None = None,
    content_type: str = "application/json",
    timeout: int = DEFAULT_REQUEST_TIMEOUT,
    allow_not_found: bool = False,
) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={
            "Content-Type": content_type,
            "Accept": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=timeout,
        ) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        response_body = exc.read()

        if allow_not_found and exc.code == 404:
            return exc.code, response_body

        raise RuntimeError(
            f"{method} {url} failed with HTTP {exc.code}: "
            f"{response_body.decode('utf-8', errors='replace')}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"{method} {url} failed: {exc.reason}"
        ) from exc


def create_index(
    es_url: str,
    index: str,
    *,
    recreate: bool,
) -> None:
    base = es_url.rstrip("/")
    encoded_index = urllib.parse.quote(index, safe="")
    index_url = f"{base}/{encoded_index}"

    if recreate:
        status, _ = http_request(
            "DELETE",
            index_url,
            allow_not_found=True,
        )

        if status not in {200, 404}:
            raise RuntimeError(
                f"Unexpected status deleting index: {status}"
            )

    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "prepare_timestamp": {"type": "date"},
                "event_type": {"type": "keyword"},
                "download_type": {"type": "keyword"},
                "repository": {"type": "keyword"},
                "resource_type": {"type": "keyword"},
                "resource_id": {"type": "keyword"},
                "client_identifier_type": {"type": "keyword"},
                "client_identifier": {"type": "keyword"},
                "retrieval_path": {
                    "type": "keyword",
                    "ignore_above": 4096,
                },
                "retrieval_status": {"type": "integer"},
                "token": {
                    "type": "keyword",
                    "index": False,
                },
                "source_file": {"type": "keyword"},
                "source_line": {"type": "integer"},
                "business_definition_version": {
                    "type": "keyword"
                },
                "dedupe_rule": {"type": "keyword"},
            }
        }
    }

    body = json.dumps(mapping).encode("utf-8")

    try:
        http_request(
            "PUT",
            index_url,
            body=body,
        )
    except RuntimeError as exc:
        message = str(exc)

        if "resource_already_exists_exception" not in message:
            raise

    LOGGER.info("Target index ready: %s", index)


def bulk_index(
    es_url: str,
    index: str,
    downloads: Sequence[CompletedDownload],
    *,
    batch_size: int = 1000,
) -> None:
    if not downloads:
        LOGGER.info("No completed downloads to index.")
        return

    base = es_url.rstrip("/")
    bulk_url = f"{base}/_bulk"

    total_indexed = 0

    for offset in range(0, len(downloads), batch_size):
        batch = downloads[offset : offset + batch_size]
        lines: list[str] = []

        for download in batch:
            lines.append(
                json.dumps(
                    {
                        "index": {
                            "_index": index,
                        }
                    },
                    separators=(",", ":"),
                )
            )
            lines.append(
                json.dumps(
                    download_to_document(download),
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
            )

        payload = ("\n".join(lines) + "\n").encode("utf-8")

        _, response_body = http_request(
            "POST",
            bulk_url,
            body=payload,
            content_type="application/x-ndjson",
        )

        response = json.loads(response_body)

        if response.get("errors"):
            failures = []

            for item in response.get("items", []):
                result = item.get("index", {})

                if "error" in result:
                    failures.append(result["error"])

                if len(failures) >= 5:
                    break

            raise RuntimeError(
                "Bulk indexing reported errors: "
                + json.dumps(failures, ensure_ascii=False)
            )

        total_indexed += len(batch)

        LOGGER.info(
            "Indexed %s of %s documents",
            f"{total_indexed:,}",
            f"{len(downloads):,}",
        )


def print_summary(
    metrics: Metrics,
    *,
    start: datetime | None,
    end: datetime | None,
    dedupe_window_seconds: int,
) -> None:
    summary = metrics_to_document(
        metrics,
        start=start,
        end=end,
        dedupe_window_seconds=dedupe_window_seconds,
    )

    print(json.dumps(summary, indent=2, sort_keys=True))


def synthetic_event(
    timestamp: str,
    path: str,
    *,
    status: int = 200,
    client_ip: str = "192.0.2.10",
    user_id: str | None = None,
    session_id: str | None = None,
    repository: str | None = None,
    resource_type: str | None = None,
    resource_id: str | None = None,
    prepare_token: str | None = None,
) -> ParsedEvent:
    parsed_timestamp = parse_iso_datetime(timestamp)

    if parsed_timestamp is None:
        raise ValueError(timestamp)

    parsed_path, query = parse_request_url(path)

    return ParsedEvent(
        timestamp=parsed_timestamp,
        method="GET",
        request=path,
        path=parsed_path,
        query=query,
        status=status,
        client_ip=client_ip,
        user_id=user_id,
        session_id=session_id,
        repository=repository,
        resource_type=resource_type,
        resource_id=resource_id,
        token=extract_token_from_retrieval_path(parsed_path),
        prepare_token=prepare_token,
        source_file="synthetic",
        source_line=1,
    )


def run_self_test() -> int:
    events = [
        # Matched prepare + retrieval: one completed download.
        synthetic_event(
            "2025-01-01T12:00:00Z",
            "/download/json/prepare",
            user_id="user-1",
            repository="NREL-USLCI",
            resource_type="repository",
            resource_id="repo-package",
            prepare_token="token-1",
        ),
        synthetic_event(
            "2025-01-01T12:00:05Z",
            "/download/json/token-1",
            user_id="user-1",
        ),

        # Retry within 60 seconds: duplicate.
        synthetic_event(
            "2025-01-01T12:00:20Z",
            "/download/json/token-1",
            user_id="user-1",
            repository="NREL-USLCI",
            resource_type="repository",
            resource_id="repo-package",
        ),

        # Same resource after more than 60 seconds: legitimate new download.
        synthetic_event(
            "2025-01-01T12:01:30Z",
            "/download/json/token-1",
            user_id="user-1",
            repository="NREL-USLCI",
            resource_type="repository",
            resource_id="repo-package",
        ),

        # Single dataset download.
        synthetic_event(
            "2025-01-01T13:00:00Z",
            "/browse/processes/process-123",
            session_id="session-1",
            repository="NREL-USLCI",
            resource_type="process",
            resource_id="process-123",
        ),

        # Dataset retry within 60 seconds: duplicate.
        synthetic_event(
            "2025-01-01T13:00:45Z",
            "/browse/processes/process-123",
            session_id="session-1",
            repository="NREL-USLCI",
            resource_type="process",
            resource_id="process-123",
        ),

        # Different dataset inside the window: legitimate.
        synthetic_event(
            "2025-01-01T13:00:50Z",
            "/browse/processes/process-456",
            session_id="session-1",
            repository="NREL-USLCI",
            resource_type="process",
            resource_id="process-456",
        ),

        # Single file download.
        synthetic_event(
            "2025-01-01T14:00:00Z",
            "/repository/file/documents/example.pdf",
            repository="NREL-USLCI",
            resource_type="file",
            resource_id="documents/example.pdf",
        ),

        # Failed request: excluded.
        synthetic_event(
            "2025-01-01T14:10:00Z",
            "/repository/file/documents/missing.pdf",
            status=404,
            repository="NREL-USLCI",
            resource_type="file",
            resource_id="documents/missing.pdf",
        ),

        # Prepare-only: excluded and counted as abandoned.
        synthetic_event(
            "2025-01-01T15:00:00Z",
            "/download/json/prepare",
            repository="NREL-USLCI",
            resource_type="repository",
            resource_id="abandoned-package",
            prepare_token="token-abandoned",
        ),

        # Unmatched token retrieval: excluded.
        synthetic_event(
            "2025-01-01T16:00:00Z",
            "/download/json/token-unknown",
            repository="NREL-USLCI",
            resource_type="repository",
            resource_id="unknown-package",
        ),
    ]

    result = process_events(
        events,
        start=parse_date_boundary(
            "2025-01-01",
            name="start date",
        ),
        end=parse_date_boundary(
            "2025-01-02",
            name="end date",
        ),
        dedupe_window_seconds=60,
        external_token_map={},
        preserve_raw_clients=True,
        client_hash_salt="",
        allow_unmatched_token_retrievals=False,
    )

    expected_downloads = 5

    print("Synthetic completed-download documents:")
    print(
        json.dumps(
            [
                download_to_document(download)
                for download in result.downloads
            ],
            indent=2,
            sort_keys=True,
        )
    )

    print("\nSynthetic metrics:")
    print(
        json.dumps(
            asdict(result.metrics),
            indent=2,
            sort_keys=True,
        )
    )

    if result.metrics.completed_downloads != expected_downloads:
        print(
            (
                "\nSELF-TEST FAILED: expected "
                f"{expected_downloads} completed downloads, got "
                f"{result.metrics.completed_downloads}"
            ),
            file=sys.stderr,
        )
        return 1

    if result.metrics.duplicate_retries != 2:
        print(
            (
                "\nSELF-TEST FAILED: expected 2 duplicate retries, got "
                f"{result.metrics.duplicate_retries}"
            ),
            file=sys.stderr,
        )
        return 1

    if result.metrics.abandoned_prepares != 1:
        print(
            (
                "\nSELF-TEST FAILED: expected 1 abandoned prepare, got "
                f"{result.metrics.abandoned_prepares}"
            ),
            file=sys.stderr,
        )
        return 1

    print("\nSELF-TEST PASSED")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    if args.self_test:
        return run_self_test()

    try:
        start = parse_date_boundary(
            args.start_date,
            name="start date",
        )
        end = parse_date_boundary(
            args.end_date,
            name="end date",
        )

        if start and end and start >= end:
            raise ValueError(
                "Start date must be earlier than end date"
            )

        external_token_map = load_token_map(
            args.prepare_token_map
        )

        metrics = Metrics()

        events = list(
            iter_log_events(
                args.logs,
                metrics,
            )
        )

        result = process_events(
            events,
            start=start,
            end=end,
            dedupe_window_seconds=args.dedupe_window_seconds,
            external_token_map=external_token_map,
            preserve_raw_clients=args.raw_client_identifiers,
            client_hash_salt=args.client_hash_salt,
            allow_unmatched_token_retrievals=(
                args.allow_unmatched_token_retrievals
            ),
            metrics=metrics,
        )

        print_summary(
            result.metrics,
            start=start,
            end=end,
            dedupe_window_seconds=args.dedupe_window_seconds,
        )

        if args.output_jsonl:
            write_jsonl(
                args.output_jsonl,
                result.downloads,
            )

        if args.summary_json:
            write_summary_json(
                args.summary_json,
                result.metrics,
                start=start,
                end=end,
                dedupe_window_seconds=args.dedupe_window_seconds,
            )

        if args.es_url and args.index:
            create_index(
                args.es_url,
                args.index,
                recreate=args.recreate,
            )

            bulk_index(
                args.es_url,
                args.index,
                result.downloads,
            )

        return 0

    except (
        FileNotFoundError,
        ValueError,
        RuntimeError,
    ) as exc:
        LOGGER.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())