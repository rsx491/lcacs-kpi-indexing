#!/usr/bin/env python3

"""
LCACS Public Repository Downloads KPI

Canonical implementation.

This script indexes public repository download events from LCACS web server
logs and produces the OpenSearch index used by the Public Repository Downloads
KPI dashboard.

Designed to be executed directly or orchestrated by the KPI framework.
Runtime parameters are supplied via command-line arguments.
"""

SCRIPT_NAME = "index_public_repo_downloads"
SCRIPT_VERSION = "1.0.0"

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

LOG_RE = re.compile(
    r'^(?P<host>\S+)\s+'
    r'(?P<client_ip>\S+)\s+\S+\s+\S+\s+'
    r'\[(?P<timestamp>[^\]]+)\]\s+'
    r'"(?P<method>[A-Z]+)\s+(?P<request>\S+)\s+HTTP/[0-9.]+"\s+'
    r'(?P<status>\d{3})\s+'
    r'(?P<bytes>\S+)\s+'
    r'"(?P<referrer>[^"]*)"\s+'
    r'"(?P<user_agent>[^"]*)"\s+'
    r'"(?P<upstream>[^"]*)"\s+'
    r'(?P<request_time>\S+)'
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
    Safely extract the request path from an access-log request target.

    Avoid urlparse() because scanner/malicious URLs can contain malformed
    bracketed host payloads that make Python's URL parser throw ValueError.
    """
    request = request.strip()

    if request.startswith("http://") or request.startswith("https://"):
        marker = "://"
        after_scheme = request.split(marker, 1)[1]
        slash_pos = after_scheme.find("/")
        request = "/" + after_scheme[slash_pos + 1:] if slash_pos >= 0 else "/"

    request = request.split("?", 1)[0]
    request = request.split("#", 1)[0]

    try:
        return unquote(request)
    except Exception:
        return request


def parse_download_endpoint(endpoint: str):
    """
    Returns dict or None.

    Supported:
      /lca-collaboration/ws/public/download/json/prepare/{group}/{repo}
      /lca-collaboration/ws/public/download/json/repository_{group}@{repo}@{commitId}

    UUID-only download URLs are not repo-identifiable from the access log alone,
    so they are intentionally skipped for this repo-scoped KPI index.
    """

    prepare_prefix = "/lca-collaboration/ws/public/download/json/prepare/"
    repository_prefix = "/lca-collaboration/ws/public/download/json/repository_"

    if endpoint.startswith(prepare_prefix):
        rest = endpoint[len(prepare_prefix):]
        parts = rest.split("/")
        if len(parts) >= 2:
            group = parts[0]
            repo = parts[1]
            return {
                "download_event_type": "download_prepare",
                "group": group,
                "repo": repo,
                "repo_path": f"{group}/{repo}",
                "commit_id": None,
                "is_repo_identifiable": True,
            }

    if endpoint.startswith(repository_prefix):
        rest = endpoint[len(repository_prefix):]
        parts = rest.split("@")
        if len(parts) >= 3:
            group = parts[0]
            repo = parts[1]
            commit_id = parts[2]
            return {
                "download_event_type": "download_json",
                "group": group,
                "repo": repo,
                "repo_path": f"{group}/{repo}",
                "commit_id": commit_id,
                "is_repo_identifiable": True,
            }

    if endpoint.startswith("/lca-collaboration/ws/public/download/json/"):
        return {
            "download_event_type": "download_json_uuid_unmapped",
            "group": None,
            "repo": None,
            "repo_path": None,
            "commit_id": None,
            "is_repo_identifiable": False,
        }

    return None


# ============================================================================
# Index Management
# ============================================================================

def create_index(es_url: str, index: str, recreate: bool = False):
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
                        "keyword": {"type": "keyword", "ignore_above": 4096}
                    },
                },
                "endpoint": {"type": "keyword"},
                "status": {"type": "integer"},
                "bytes": {"type": "long"},
                "referrer": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 4096}
                    },
                },
                "user_agent": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 4096}
                    },
                },
                "upstream": {"type": "keyword"},
                "request_time": {"type": "float"},

                "download_event_type": {"type": "keyword"},
                "group": {"type": "keyword"},
                "repo": {"type": "keyword"},
                "repo_path": {"type": "keyword"},
                "commit_id": {"type": "keyword"},

                "is_public_repo": {"type": "boolean"},
                "is_repo_identifiable": {"type": "boolean"},

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
        requests.delete(f"{es_url}/{index}", timeout=30)

    exists = requests.head(f"{es_url}/{index}", timeout=30).status_code == 200
    if exists:
        print(f"Index already exists: {index}")
        return

    resp = requests.put(f"{es_url}/{index}", json=mapping, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Create index failed: {resp.status_code} {resp.text}")

    print(f"Created index: {index}")


# ============================================================================
# Output
# ============================================================================

def bulk_index(es_url: str, index: str, docs: list):
    if not docs:
        return

    lines = []
    for doc in docs:
        raw_id = (
            f"{doc['@timestamp']}|{doc['client_ip']}|{doc['method']}|"
            f"{doc['endpoint']}|{doc['status']}|{doc['request_time']}"
        )
        doc_id = hashlib.sha1(raw_id.encode("utf-8")).hexdigest()

        lines.append({"index": {"_index": index, "_id": doc_id}})
        lines.append(doc)

    payload = "\n".join(json.dumps(x) for x in lines) + "\n"

    resp = requests.post(
        f"{es_url}/_bulk",
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=120,
    )

    if not resp.ok:
        raise RuntimeError(f"Bulk failed: {resp.status_code} {resp.text}")

    result = resp.json()
    if result.get("errors"):
        raise RuntimeError(f"Bulk had errors: {json.dumps(result)[:3000]}")


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
    download_lines_seen = 0

    indexed_public_events = 0
    skipped_non_public = 0
    skipped_unmapped_uuid = 0
    skipped_unparseable = 0

    successful_prepare = 0
    successful_download_json = 0
    successful_total = 0

    event_type_counts = {}
    repo_counts = {}

    batch = []
    generated_at = datetime.now(timezone.utc).isoformat()

    with open_log(log_file) as f:
        for line in f:
            total_lines += 1

            m = LOG_RE.match(line)
            if not m:
                continue

            row = m.groupdict()
            request = row["request"]
            endpoint = normalize_endpoint(request)

            if "/lca-collaboration/ws/public/download/json/" not in endpoint:
                continue

            download_lines_seen += 1

            parsed = parse_download_endpoint(endpoint)
            if not parsed:
                skipped_unparseable += 1
                continue

            if not parsed["is_repo_identifiable"]:
                skipped_unmapped_uuid += 1
                continue

            repo = parsed["repo"]
            is_public_repo = repo in PUBLIC_REPOS

            if not is_public_repo:
                skipped_non_public += 1
                continue

            status = int(row["status"])
            is_success = 200 <= status < 300

            bytes_value = 0 if row["bytes"] == "-" else int(row["bytes"])
            request_time = 0.0 if row["request_time"] == "-" else float(row["request_time"])

            doc = {
                "@timestamp": parse_timestamp(row["timestamp"]),
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

                "download_event_type": parsed["download_event_type"],
                "group": parsed["group"],
                "repo": parsed["repo"],
                "repo_path": parsed["repo_path"],
                "commit_id": parsed["commit_id"],

                "is_public_repo": True,
                "is_repo_identifiable": True,

                "source": "lcacs_web_logs",
                "kpi_name": "public_repo_download_counts",
                "script_name": SCRIPT_NAME,
                "script_version": SCRIPT_VERSION,

                "run_label": run_label,
                "kpi_period_start": start_date,
                "kpi_period_end": end_date,
                "generated_at": generated_at,
            }

            indexed_public_events += 1

            event_type = parsed["download_event_type"]
            event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
            repo_counts[parsed["repo_path"]] = repo_counts.get(parsed["repo_path"], 0) + 1

            if is_success:
                successful_total += 1
                if event_type == "download_prepare":
                    successful_prepare += 1
                elif event_type == "download_json":
                    successful_download_json += 1

            batch.append(doc)

            if len(batch) >= batch_size:
                if not dry_run:
                    bulk_index(es_url, index, batch)
                batch.clear()

    if batch:
        if not dry_run:
            bulk_index(es_url, index, batch)

    print("\n=== PUBLIC REPO DOWNLOAD INDEXING SUMMARY ===")
    print(f"Script: {SCRIPT_NAME} {SCRIPT_VERSION}")
    print(f"Run label: {run_label}")
    print(f"KPI period: {start_date} to {end_date}")
    print(f"Dry run: {dry_run}")

    print(f"Total raw log lines read: {total_lines:,}")
    print(f"Download/json lines seen: {download_lines_seen:,}")
    print(f"Indexed public repo download events: {indexed_public_events:,}")

    print(f"Skipped non-public repo events: {skipped_non_public:,}")
    print(f"Skipped UUID download_json events without repo mapping: {skipped_unmapped_uuid:,}")
    print(f"Skipped unparseable events: {skipped_unparseable:,}")

    print(f"Successful public repo download events total 2xx: {successful_total:,}")
    print(f"Successful public repo prepare events 2xx: {successful_prepare:,}")
    print(f"Successful public repo actual JSON download events 2xx: {successful_download_json:,}")

    print("\nEvent type counts:")
    for event_type, count in sorted(event_type_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {event_type}: {count:,}")

    print("\nTop 20 public repo download counts:")
    for repo_path, count in sorted(repo_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"  {count:,}\t{repo_path}")


# ============================================================================
# Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Index LCACS public repository download counts."
    )
    parser.add_argument("log_file", help="Path to a combined LCACS access log file, plain text or .gz")
    parser.add_argument("--es-url", default=DEFAULT_ES_URL)
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--start-date", help="Inclusive start date, YYYY-MM-DD")
    parser.add_argument("--end-date", help="Exclusive end date, YYYY-MM-DD")
    parser.add_argument("--run-label", default="manual")
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log_file = Path(args.log_file)
    if not log_file.exists():
        raise SystemExit(f"File not found: {log_file}")

    create_index(args.es_url, args.index, recreate=args.recreate)
    parse_log_file(
        log_file,
        args.es_url,
        args.index,
        start_date=args.start_date,
        end_date=args.end_date,
        run_label=args.run_label,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()