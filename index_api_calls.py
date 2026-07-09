#!/usr/bin/env python3

"""
LCACS API Calls KPI

Canonical implementation.

This script indexes LCACS public API requests from web server logs and
produces the OpenSearch index used by the API Calls KPI dashboard.

Designed to be executed directly or orchestrated by the KPI framework.
Runtime parameters (index names, ES URL, etc.) are supplied via
command-line arguments.
"""

SCRIPT_NAME = "index_api_calls"
SCRIPT_VERSION = "1.0.0"

import argparse
import gzip
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests


DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_INDEX = "lcacs-kpi-api-calls"

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

TS_FORMAT = "%d/%b/%Y:%H:%M:%S %z"


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


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:16]


def normalize_endpoint(request: str) -> str:
    return request.split("?", 1)[0]


def redact_api_key(request: str) -> str:
    parsed = urlparse(request)
    if not parsed.query:
        return request

    parts = []
    for pair in parsed.query.split("&"):
        if pair.lower().startswith("api_key="):
            parts.append("api_key=REDACTED")
        else:
            parts.append(pair)

    return parsed.path + "?" + "&".join(parts)


def classify_endpoint(endpoint: str):
    if endpoint == "/lca-collaboration/ws/public/search":
        return "search"

    if endpoint.startswith("/lca-collaboration/ws/public/browse/"):
        return "browse"

    if endpoint.startswith("/lca-collaboration/ws/public/download/json/prepare/"):
        return "download_prepare"

    if endpoint.startswith("/lca-collaboration/ws/public/download/json/"):
        return "download_json"

    if endpoint.startswith("/lca-collaboration/ws/public/repository/file/"):
        return "repository_file"

    return None

def is_documented_api_endpoint_group(group: str) -> bool:
    return group in {
        "search",
        "browse",
        "download_prepare",
        "download_json",
        "repository_file",
    }


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
                "request_redacted": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 4096}
                    },
                },

                "endpoint": {"type": "keyword"},
                "api_endpoint_group": {"type": "keyword"},
                "is_documented_api_endpoint": {"type": "boolean"},

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

                "is_public_api_call": {"type": "boolean"},
                "has_api_key": {"type": "boolean"},
                "api_auth_type": {"type": "keyword"},
                "api_key_hash": {"type": "keyword"},

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
            f"{doc['request_redacted']}|{doc['status']}|{doc['request_time']}"
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
    public_api_calls = 0

    token_calls = 0
    token_success = 0
    non_token_calls = 0
    non_token_success = 0

    documented_calls = 0
    documented_success = 0

    group_counts = {}
    status_counts = {}

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

            if "/lca-collaboration/ws/public/" not in request:
                continue

            endpoint = normalize_endpoint(request)
            endpoint_group = classify_endpoint(endpoint)
            if endpoint_group is None:
                continue
            is_documented = is_documented_api_endpoint_group(endpoint_group)

            parsed = urlparse(request)
            qs = parse_qs(parsed.query)

            api_key = None
            if "api_key" in qs and qs["api_key"]:
                api_key = qs["api_key"][0]

            has_api_key = bool(api_key)
            api_auth_type = "token" if has_api_key else "non_token"

            status = int(row["status"])
            is_success = 200 <= status < 300

            public_api_calls += 1

            if is_documented:
                documented_calls += 1
                if is_success:
                    documented_success += 1

            if has_api_key:
                token_calls += 1
                if is_success:
                    token_success += 1
            else:
                non_token_calls += 1
                if is_success:
                    non_token_success += 1

            group_counts[endpoint_group] = group_counts.get(endpoint_group, 0) + 1
            status_counts[status] = status_counts.get(status, 0) + 1

            bytes_value = 0 if row["bytes"] == "-" else int(row["bytes"])
            request_time = 0.0 if row["request_time"] == "-" else float(row["request_time"])

            doc = {
                "@timestamp": parse_timestamp(row["timestamp"]),

                "host": row["host"],
                "client_ip": row["client_ip"],
                "method": row["method"],

                "request": request,
                "request_redacted": redact_api_key(request),

                "endpoint": endpoint,
                "api_endpoint_group": endpoint_group,
                "is_documented_api_endpoint": is_documented,

                "status": status,
                "bytes": bytes_value,

                "referrer": row["referrer"],
                "user_agent": row["user_agent"],
                "upstream": row["upstream"],
                "request_time": request_time,

                "is_public_api_call": True,
                "has_api_key": has_api_key,
                "api_auth_type": api_auth_type,
                "api_key_hash": hash_api_key(api_key) if has_api_key else None,

                "source": "lcacs_web_logs",
                "kpi_name": "api_calls_token_vs_non_token",
                "script_name": SCRIPT_NAME,
                "script_version": SCRIPT_VERSION,

                "run_label": run_label,
                "kpi_period_start": start_date,
                "kpi_period_end": end_date,
                "generated_at": generated_at,
            }

            batch.append(doc)

            if len(batch) >= batch_size:
                if not dry_run:
                    bulk_index(es_url, index, batch)
                batch.clear()

    if batch:
        if not dry_run:
            bulk_index(es_url, index, batch)

    print("\n=== API CALL INDEXING SUMMARY ===")
    print(f"Script: {SCRIPT_NAME} {SCRIPT_VERSION}")
    print(f"Run label: {run_label}")
    print(f"KPI period: {start_date} to {end_date}")
    print(f"Dry run: {dry_run}")
    print(f"Total raw log lines read: {total_lines:,}")
    print(f"Public /ws/public calls indexed: {public_api_calls:,}")

    print(f"Documented public API endpoint calls: {documented_calls:,}")
    print(f"Successful documented public API endpoint calls 2xx: {documented_success:,}")

    print(f"Token API calls has api_key=: {token_calls:,}")
    print(f"Successful token API calls 2xx: {token_success:,}")

    print(f"Non-token API calls: {non_token_calls:,}")
    print(f"Successful non-token API calls 2xx: {non_token_success:,}")

    print("\nStatus counts:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count:,}")

    print("\nEndpoint group counts:")
    for group, count in sorted(group_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {group}: {count:,}")


# ============================================================================
# Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Index LCACS public API calls with token/non-token and endpoint-group classification."
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