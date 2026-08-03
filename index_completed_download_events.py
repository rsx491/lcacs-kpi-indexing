#!/usr/bin/env python3

"""
LCACS Public Repository Downloads KPI

Canonical implementation.

This script indexes successful completed-download events for LCACS repositories
that are anonymously exposed to the public.

Canonical event definition:

  - Request path begins with:
      /lca-collaboration/ws/public/download/json/

  - Request path does not contain:
      /prepare/

  - HTTP response status is successful (2xx)

  - The completed-download URL identifies a repository using:
      repository_{group}@{repo}@{commit_id}

  - The repository is present in the authoritative PUBLIC_REPOS set

UUID-only completed-download URLs are real completed-download events, but they
cannot be attributed to a repository using the completed request alone. They
are therefore excluded from this repository-scoped KPI and reported separately
in the execution summary.

Designed to be executed directly or orchestrated by the KPI framework.
Runtime parameters are supplied through command-line arguments.
"""

SCRIPT_NAME = "index_public_repo_downloads"
SCRIPT_VERSION = "1.1.0"

import argparse
import gzip
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

import requests


DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_INDEX = "lcacs-kpi-public-repo-downloads"

TS_FORMAT = "%d/%b/%Y:%H:%M:%S %z"

DOWNLOAD_PREFIX = "/lca-collaboration/ws/public/download/json/"
PREPARE_PREFIX = f"{DOWNLOAD_PREFIX}prepare/"
REPOSITORY_PREFIX = f"{DOWNLOAD_PREFIX}repository_"

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)

LOG_RE = re.compile(
    r"^(?P<host>\S+)\s+"
    r"(?P<client_ip>\S+)\s+\S+\s+\S+\s+"
    r"\[(?P<timestamp>[^\]]+)\]\s+"
    r'"(?P<method>[A-Z]+)\s+(?P<request>\S+)\s+HTTP/[0-9.]+"\s+'
    r"(?P<status>\d{3})\s+"
    r"(?P<bytes>\S+)\s+"
    r'"(?P<referrer>[^"]*)"\s+'
    r'"(?P<user_agent>[^"]*)"\s+'
    r'"(?P<upstream>[^"]*)"\s+'
    r"(?P<request_time>\S+)"
)

PUBLIC_REPOS = {
    "ReCiPe",
    "elementary_flow_list",
    "Field_crop_production",
    "CED_Method",
    "Fed_Commons_core_database",
    "USEEIO_v2",
    "US_electricity_baseline",
    "Swine",
    "mtu_pavement",
    "Coal_extraction",
    "Beef_production",
    "Construction_and_demolition_2022_update_2",
    "TRACI",
    "Heavy_equipment_operation",
    "Forestry_and_forest_products",
    "USEEIO",
    "USLCI_Database_Public",
    "Impact_World_Plus",
    "IPCC_GWP",
    "FEDEFL_Inv",
    "construction_epd_indicators",
    "Concrete",
    "Kraft_pulp",
    "TRACI_2_2",
    "Woody_biomass",
    "construction_materials",
    "Building_Systems",
}


# ============================================================================
# Log Parsing Helpers
# ============================================================================

def open_log(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", errors="replace")

    return path.open("rt", errors="replace")


def parse_timestamp(raw: str) -> str:
    dt = datetime.strptime(raw, TS_FORMAT)
    return dt.astimezone(timezone.utc).isoformat()


def normalize_endpoint(request: str) -> str:
    """
    Extract and normalize the request path from an access-log request target.

    urlparse() is deliberately avoided because malformed scanner traffic can
    contain bracketed host payloads that cause it to raise ValueError.

    URL decoding also normalizes values such as `%40` to `@`, allowing encoded
    repository identifiers to be parsed consistently.
    """
    request = request.strip()

    if request.startswith("http://") or request.startswith("https://"):
        after_scheme = request.split("://", 1)[1]
        slash_pos = after_scheme.find("/")

        if slash_pos >= 0:
            request = "/" + after_scheme[slash_pos + 1:]
        else:
            request = "/"

    request = request.split("?", 1)[0]
    request = request.split("#", 1)[0]

    try:
        return unquote(request)
    except Exception:
        return request


def parse_download_endpoint(endpoint: str):
    """
    Parse an LCACS public-download endpoint.

    Returns a dictionary for recognized download routes or None when the route
    cannot be classified.

    Recognized endpoint families:

      Preparation request:
        /lca-collaboration/ws/public/download/json/prepare/{group}/{repo}

      Repository-identifiable completed download:
        /lca-collaboration/ws/public/download/json/
            repository_{group}@{repo}@{commit_id}

      UUID-only completed download:
        /lca-collaboration/ws/public/download/json/{uuid}

    Preparation requests are classified but never indexed.

    UUID-only downloads are classified but cannot be included in this
    repository-scoped KPI without an external correlation to the corresponding
    preparation request.
    """
    endpoint = unquote(endpoint)

    if not endpoint.startswith(DOWNLOAD_PREFIX):
        return None

    if endpoint.startswith(PREPARE_PREFIX):
        rest = endpoint[len(PREPARE_PREFIX):].strip("/")
        parts = rest.split("/")

        group = parts[0] if len(parts) >= 1 and parts[0] else None
        repo = parts[1] if len(parts) >= 2 and parts[1] else None

        return {
            "is_completed_download": False,
            "download_event_type": "download_prepare",
            "group": group,
            "repo": repo,
            "repo_path": f"{group}/{repo}" if group and repo else None,
            "commit_id": None,
            "download_token": None,
            "is_repo_identifiable": bool(group and repo),
        }

    identifier = endpoint[len(DOWNLOAD_PREFIX):].strip("/")

    if identifier.startswith("repository_"):
        repository_identifier = identifier[len("repository_"):]
        parts = repository_identifier.split("@", 2)

        if len(parts) != 3:
            return None

        group, repo, commit_id = parts

        if not group or not repo or not commit_id:
            return None

        return {
            "is_completed_download": True,
            "download_event_type": "repository_download",
            "group": group,
            "repo": repo,
            "repo_path": f"{group}/{repo}",
            "commit_id": commit_id,
            "download_token": identifier,
            "is_repo_identifiable": True,
        }

    if UUID_RE.fullmatch(identifier):
        return {
            "is_completed_download": True,
            "download_event_type": "uuid_download_unmapped",
            "group": None,
            "repo": None,
            "repo_path": None,
            "commit_id": None,
            "download_token": identifier,
            "is_repo_identifiable": False,
        }

    return None


# ============================================================================
# Index Management
# ============================================================================

def create_index(
    es_url: str,
    index: str,
    recreate: bool = False,
):
    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "host": {"type": "keyword"},
                "client_ip": {"type": "ip"},
                "method": {"type": "keyword"},
                "request": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 4096,
                        }
                    },
                },
                "endpoint": {
                    "type": "keyword",
                    "ignore_above": 4096,
                },
                "status": {"type": "integer"},
                "bytes": {"type": "long"},
                "referrer": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 4096,
                        }
                    },
                },
                "user_agent": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 4096,
                        }
                    },
                },
                "upstream": {"type": "keyword"},
                "request_time": {"type": "float"},

                "download_event_type": {"type": "keyword"},
                "download_token": {
                    "type": "keyword",
                    "ignore_above": 4096,
                },
                "is_completed_download": {"type": "boolean"},

                "group": {"type": "keyword"},
                "repo": {"type": "keyword"},
                "repo_path": {"type": "keyword"},
                "commit_id": {"type": "keyword"},

                "is_public_repo": {"type": "boolean"},
                "is_repo_identifiable": {"type": "boolean"},

                "source_log": {
                    "type": "keyword",
                    "ignore_above": 4096,
                },
                "source_line": {"type": "long"},

                "source": {"type": "keyword"},
                "kpi_name": {"type": "keyword"},
                "script_name": {"type": "keyword"},
                "script_version": {"type": "keyword"},

                "run_label": {"type": "keyword"},
                "kpi_period_start": {"type": "date"},
                "kpi_period_end": {"type": "date"},
                "generated_at": {"type": "date"},
            }
        }
    }

    if recreate:
        response = requests.delete(
            f"{es_url}/{index}",
            timeout=30,
        )

        if response.status_code not in (200, 404):
            raise RuntimeError(
                f"Delete index failed: "
                f"{response.status_code} {response.text}"
            )

    exists = (
        requests.head(
            f"{es_url}/{index}",
            timeout=30,
        ).status_code
        == 200
    )

    if exists:
        print(f"Index already exists: {index}")
        return

    response = requests.put(
        f"{es_url}/{index}",
        json=mapping,
        timeout=30,
    )

    if not response.ok:
        raise RuntimeError(
            f"Create index failed: "
            f"{response.status_code} {response.text}"
        )

    print(f"Created index: {index}")


# ============================================================================
# Output
# ============================================================================

def bulk_index(
    es_url: str,
    index: str,
    docs: list,
):
    if not docs:
        return

    lines = []

    for doc in docs:
        # Source file and source-line number make indexing idempotent while
        # preserving separate raw events that otherwise happen to share the
        # same timestamp, IP address, endpoint, and response time.
        raw_id = (
            f"{doc['source_log']}|"
            f"{doc['source_line']}|"
            f"{doc['@timestamp']}|"
            f"{doc['endpoint']}"
        )

        doc_id = hashlib.sha1(
            raw_id.encode("utf-8")
        ).hexdigest()

        lines.append(
            {
                "index": {
                    "_index": index,
                    "_id": doc_id,
                }
            }
        )
        lines.append(doc)

    payload = "\n".join(
        json.dumps(item) for item in lines
    ) + "\n"

    response = requests.post(
        f"{es_url}/_bulk",
        data=payload,
        headers={
            "Content-Type": "application/x-ndjson"
        },
        timeout=120,
    )

    if not response.ok:
        raise RuntimeError(
            f"Bulk request failed: "
            f"{response.status_code} {response.text}"
        )

    result = response.json()

    if result.get("errors"):
        failed_items = []

        for item in result.get("items", []):
            operation = item.get("index", {})

            if "error" in operation:
                failed_items.append(operation)

            if len(failed_items) >= 10:
                break

        raise RuntimeError(
            "Bulk request contained document errors: "
            f"{json.dumps(failed_items, indent=2)[:5000]}"
        )


# ============================================================================
# KPI Construction
# ============================================================================

def parse_log_file(
    log_file: Path,
    es_url: str,
    index: str,
    batch_size: int = 5000,
    start_date: str = None,
    end_date: str = None,
    run_label: str = "manual",
    dry_run: bool = False,
):
    total_lines = 0
    parsed_log_lines = 0
    download_route_lines = 0

    indexed_public_downloads = 0

    skipped_prepare = 0
    skipped_unsuccessful = 0
    skipped_unmapped_uuid = 0
    skipped_non_public = 0
    skipped_unparseable_endpoint = 0
    skipped_unparseable_log_line = 0

    event_type_counts = {}
    repo_counts = {}

    batch = []
    generated_at = datetime.now(timezone.utc).isoformat()
    source_log = str(log_file.resolve())

    with open_log(log_file) as file_handle:
        for source_line, line in enumerate(
            file_handle,
            start=1,
        ):
            total_lines += 1

            match = LOG_RE.match(line)

            if not match:
                skipped_unparseable_log_line += 1
                continue

            parsed_log_lines += 1
            row = match.groupdict()

            request = row["request"]
            endpoint = normalize_endpoint(request)

            if not endpoint.startswith(DOWNLOAD_PREFIX):
                continue

            download_route_lines += 1

            parsed = parse_download_endpoint(endpoint)

            if not parsed:
                skipped_unparseable_endpoint += 1
                continue

            # Preparation requests initiate artifact creation but do not
            # represent delivery of a completed download.
            if not parsed["is_completed_download"]:
                skipped_prepare += 1
                continue

            status = int(row["status"])

            # Canonical KPI includes only successful completed requests.
            if not 200 <= status < 300:
                skipped_unsuccessful += 1
                continue

            # UUID-only download requests are completed transfers, but the
            # completed request does not identify the repository. They cannot
            # be safely attributed to a public repository without correlation
            # to another event and are excluded from this repo-scoped KPI.
            if not parsed["is_repo_identifiable"]:
                skipped_unmapped_uuid += 1
                continue

            repo = parsed["repo"]

            # The KPI intentionally includes only repositories known to be
            # anonymously exposed to the public.
            if repo not in PUBLIC_REPOS:
                skipped_non_public += 1
                continue

            try:
                bytes_value = (
                    0
                    if row["bytes"] == "-"
                    else int(row["bytes"])
                )
            except ValueError:
                bytes_value = 0

            try:
                request_time = (
                    0.0
                    if row["request_time"] == "-"
                    else float(row["request_time"])
                )
            except ValueError:
                request_time = 0.0

            document = {
                "@timestamp": parse_timestamp(
                    row["timestamp"]
                ),
                "host": row["host"],
                "client_ip": row["client_ip"],
                "method": row["method"],
                "request": request,
                "endpoint": endpoint,
                "status": status,
                "bytes": bytes_value,
                "referrer": row["referrer"],
                "user_agent": row["user_agent"],
                "upstream": row["upstream"],
                "request_time": request_time,

                "download_event_type": (
                    parsed["download_event_type"]
                ),
                "download_token": parsed["download_token"],
                "is_completed_download": True,

                "group": parsed["group"],
                "repo": parsed["repo"],
                "repo_path": parsed["repo_path"],
                "commit_id": parsed["commit_id"],

                "is_public_repo": True,
                "is_repo_identifiable": True,

                "source_log": source_log,
                "source_line": source_line,

                "source": "lcacs_web_logs",
                "kpi_name": (
                    "public_repo_completed_download_events"
                ),
                "script_name": SCRIPT_NAME,
                "script_version": SCRIPT_VERSION,

                "run_label": run_label,
                "kpi_period_start": start_date,
                "kpi_period_end": end_date,
                "generated_at": generated_at,
            }

            indexed_public_downloads += 1

            event_type = parsed["download_event_type"]
            event_type_counts[event_type] = (
                event_type_counts.get(event_type, 0) + 1
            )

            repo_path = parsed["repo_path"]
            repo_counts[repo_path] = (
                repo_counts.get(repo_path, 0) + 1
            )

            batch.append(document)

            if len(batch) >= batch_size:
                if not dry_run:
                    bulk_index(
                        es_url,
                        index,
                        batch,
                    )

                batch.clear()

    if batch and not dry_run:
        bulk_index(
            es_url,
            index,
            batch,
        )

    print(
        "\n"
        "=== PUBLIC REPOSITORY COMPLETED-DOWNLOAD "
        "INDEXING SUMMARY ==="
    )
    print(f"Script: {SCRIPT_NAME} {SCRIPT_VERSION}")
    print(f"Run label: {run_label}")
    print(f"KPI period: {start_date} to {end_date}")
    print(f"Dry run: {dry_run}")

    print(f"\nTotal raw log lines read: {total_lines:,}")
    print(f"Successfully parsed log lines: {parsed_log_lines:,}")
    print(f"Download-route lines seen: {download_route_lines:,}")

    print(
        "Indexed successful completed downloads for public repos: "
        f"{indexed_public_downloads:,}"
    )

    print(f"\nExcluded preparation requests: {skipped_prepare:,}")
    print(
        "Excluded unsuccessful completed requests: "
        f"{skipped_unsuccessful:,}"
    )
    print(
        "Excluded UUID completed downloads without repo mapping: "
        f"{skipped_unmapped_uuid:,}"
    )
    print(
        "Excluded repository-identifiable downloads for repos "
        f"not in PUBLIC_REPOS: {skipped_non_public:,}"
    )
    print(
        "Unparseable download endpoint records: "
        f"{skipped_unparseable_endpoint:,}"
    )
    print(
        "Unparseable raw access-log records: "
        f"{skipped_unparseable_log_line:,}"
    )

    print("\nIndexed event-type counts:")

    for event_type, count in sorted(
        event_type_counts.items(),
        key=lambda item: item[1],
        reverse=True,
    ):
        print(f"  {event_type}: {count:,}")

    print("\nTop 20 public repository completed-download counts:")

    for repo_path, count in sorted(
        repo_counts.items(),
        key=lambda item: item[1],
        reverse=True,
    )[:20]:
        print(f"  {count:,}\t{repo_path}")


# ============================================================================
# Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Index successful completed-download events for "
            "anonymously accessible LCACS public repositories."
        )
    )

    parser.add_argument(
        "log_file",
        help=(
            "Path to a combined LCACS access log file, "
            "plain text or .gz"
        ),
    )
    parser.add_argument(
        "--es-url",
        default=DEFAULT_ES_URL,
    )
    parser.add_argument(
        "--index",
        default=DEFAULT_INDEX,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
    )
    parser.add_argument(
        "--start-date",
        help="Inclusive KPI start date, YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        help="Exclusive KPI end date, YYYY-MM-DD",
    )
    parser.add_argument(
        "--run-label",
        default="manual",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    args = parser.parse_args()

    log_file = Path(args.log_file)

    if not log_file.exists():
        raise SystemExit(f"File not found: {log_file}")

    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be greater than zero")

    # A dry run parses and reports results without changing Elasticsearch.
    if not args.dry_run:
        create_index(
            args.es_url,
            args.index,
            recreate=args.recreate,
        )

    parse_log_file(
        log_file=log_file,
        es_url=args.es_url,
        index=args.index,
        batch_size=args.batch_size,
        start_date=args.start_date,
        end_date=args.end_date,
        run_label=args.run_label,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()