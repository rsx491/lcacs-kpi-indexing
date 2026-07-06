#!/usr/bin/env python3

import argparse
import gzip
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests


REQUEST_RE = re.compile(r'"([A-Z]+)\s+([^"]+)\s+HTTP/[0-9.]+"\s+(\d{3})')
DATE_RE = re.compile(r"\[(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})")

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

RELEASE_PREFIX = "/lca-collaboration/ws/release/"


def open_log(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", errors="replace")
    return path.open("rt", errors="replace")


def parse_timestamp(line: str):
    match = DATE_RE.search(line)
    if not match:
        return None

    day, month, year, hour, minute, second = match.groups()

    return datetime(
        int(year),
        MONTHS[month],
        int(day),
        int(hour),
        int(minute),
        int(second),
    )


def parse_request(line: str):
    match = REQUEST_RE.search(line)
    if not match:
        return None, None, None

    method, request_path, status = match.groups()
    return method, request_path, int(status)


def parse_release_path(request_path: str):
    """
    Expected:
      /lca-collaboration/ws/release/{group}/{repo}/{commitId}

    Returns:
      group, repo, commit_id
    """
    clean_path = request_path.split("?", 1)[0]

    if not clean_path.startswith(RELEASE_PREFIX):
        return None, None, None

    tail = clean_path[len(RELEASE_PREFIX):].strip("/")
    parts = tail.split("/")

    if len(parts) < 3:
        return None, None, None

    group = parts[0]
    repo = parts[1]
    commit_id = parts[2]

    return group, repo, commit_id


def classify_release_activity(method: str, status: int):
    """
    KPI logic:
      POST 200 = release creation / publish event
      PUT  200 = release edit/update
      GET  200 = release info view/fetch
      non-200 = excluded from main KPI counts, kept as diagnostics
    """
    if status != 200:
        return "release_endpoint_non_200"

    if method == "POST":
        return "release_creation"

    if method == "PUT":
        return "release_update"

    if method == "GET":
        return "release_info_view"

    return "release_endpoint_other_200"


def parse_log(log_path: Path):
    counts = Counter()
    by_repo = defaultdict(Counter)
    events = []

    total_lines = 0
    release_endpoint_lines = 0

    first_ts = None
    last_ts = None

    with open_log(log_path) as f:
        for line in f:
            total_lines += 1

            timestamp = parse_timestamp(line)
            if timestamp:
                if first_ts is None or timestamp < first_ts:
                    first_ts = timestamp
                if last_ts is None or timestamp > last_ts:
                    last_ts = timestamp

            method, request_path, status = parse_request(line)
            if not request_path:
                continue

            if RELEASE_PREFIX not in request_path:
                continue

            group, repo, commit_id = parse_release_path(request_path)
            activity_type = classify_release_activity(method, status)

            release_endpoint_lines += 1 
            counts[activity_type] += 1

            repository_path = f"{group}/{repo}" if group and repo else "unknown"
            by_repo[repository_path][activity_type] += 1

            if timestamp:
                ts_utc = timestamp.replace(tzinfo=timezone.utc)
                timestamp_iso = ts_utc.isoformat()
                event_date = timestamp.strftime("%Y-%m-%d")
                event_timestamp_display = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            else:
                timestamp_iso = None
                event_date = None
                event_timestamp_display = None

            events.append({
                "@timestamp": timestamp_iso,
                "event_date": event_date,
                "event_timestamp_display": event_timestamp_display,
                "method": method,
                "status": status,
                "request_path": request_path,
                "repository_path": repository_path,
                "agency": group,
                "repository": repo,
                "commit_id": commit_id,
                "activity_type": activity_type,
            })

    print(f"DEBUG release_endpoint_lines={release_endpoint_lines}, events={len(events)}")

    return {
        "total_lines": total_lines,
        "release_endpoint_lines": release_endpoint_lines,
        "first_timestamp": first_ts.replace(tzinfo=timezone.utc).isoformat() if first_ts else None,
        "last_timestamp": last_ts.replace(tzinfo=timezone.utc).isoformat() if last_ts else None,
        "counts": counts,
        "by_repo": by_repo,
        "events": events,
    }


def es_request(method: str, es_url: str, path: str, **kwargs):
    url = f"{es_url.rstrip('/')}/{path.lstrip('/')}"
    resp = requests.request(method, url, timeout=120, **kwargs)

    if not resp.ok:
        raise RuntimeError(f"{method} {url} failed: {resp.status_code} {resp.text}")

    return resp.json() if resp.text else {}


def create_summary_index(es_url: str, index_name: str):
    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "window_start": {"type": "date"},
                "window_end": {"type": "date"},
                "source_log_file": {"type": "keyword"},

                "total_lines": {"type": "long"},
                "release_endpoint_lines": {"type": "long"},

                "release_creations": {"type": "long"},
                "release_updates": {"type": "long"},
                "release_info_views": {"type": "long"},
                "release_endpoint_non_200": {"type": "long"},
                "release_endpoint_other_200": {"type": "long"},

                "metric_name": {"type": "keyword"},
                "metric_group": {"type": "keyword"},
                "metric_note": {"type": "text"},
                "calculation": {"type": "keyword"},
            }
        }
    }

    exists = requests.head(f"{es_url.rstrip('/')}/{index_name}", timeout=30).status_code == 200

    if not exists:
        es_request("PUT", es_url, index_name, json=mapping)
        print(f"Created index: {index_name}")
    else:
        print(f"Index already exists: {index_name}")


def create_events_index(es_url: str, index_name: str):
    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "event_date": {"type": "keyword"},
                "event_timestamp_display": {"type": "keyword"},
                "method": {"type": "keyword"},
                "status": {"type": "integer"},
                "request_path": {"type": "keyword"},
                "repository_path": {"type": "keyword"},
                "agency": {"type": "keyword"},
                "repository": {"type": "keyword"},
                "commit_id": {"type": "keyword"},
                "activity_type": {"type": "keyword"},
            }
        }
    }

    exists = requests.head(f"{es_url.rstrip('/')}/{index_name}", timeout=30).status_code == 200

    if not exists:
        es_request("PUT", es_url, index_name, json=mapping)
        print(f"Created index: {index_name}")
    else:
        print(f"Index already exists: {index_name}")

def index_summary(es_url: str, index_name: str, doc: dict, doc_id: str):
    es_request("PUT", es_url, f"{index_name}/_doc/{doc_id}", json=doc)
    print(f"Indexed summary doc into {index_name}: {doc_id}")


def bulk_index_events(es_url: str, index_name: str, events: list):
    if not events:
        print("No release events to index.")
        return

    lines = []

    for i, event in enumerate(events):
        doc_id = f"{event.get('@timestamp')}-{event.get('method')}-{event.get('commit_id')}-{i}"
        lines.append({"index": {"_index": index_name, "_id": doc_id}})
        lines.append(event)

    payload = "\n".join(json.dumps(x) for x in lines) + "\n"

    resp = requests.post(
        f"{es_url.rstrip('/')}/_bulk",
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=120,
    )

    if not resp.ok:
        raise RuntimeError(f"Bulk index failed: {resp.status_code} {resp.text}")

    result = resp.json()

    if result.get("errors"):
        raise RuntimeError(f"Bulk index had errors: {json.dumps(result, indent=2)}")

    print(f"Indexed {len(events)} release event docs into {index_name}")


def main():
    parser = argparse.ArgumentParser(
        description="Parse LCACS release endpoint activity from web logs and index KPI fields into OpenSearch."
    )

    parser.add_argument("log_file", help="Path to logs_30d_all.log or logs_30d_all.log.gz")
    parser.add_argument("--es-url", default="http://localhost:9200")
    parser.add_argument("--summary-index", default="lcacs-kpi-release-activity-30d")
    parser.add_argument("--events-index", default="lcacs-kpi-release-activity-events-30d")
    parser.add_argument("--doc-id", default="release-activity-30d-current")
    parser.add_argument("--skip-events", action="store_true", help="Only index the summary document, not individual events.")

    args = parser.parse_args()

    log_path = Path(args.log_file)

    if not log_path.exists():
        raise SystemExit(f"File not found: {log_path}")

    parsed = parse_log(log_path)
    counts = parsed["counts"]

    summary_doc = {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "window_start": parsed["first_timestamp"],
        "window_end": parsed["last_timestamp"],
        "source_log_file": str(log_path),

        "total_lines": parsed["total_lines"],
        "release_endpoint_lines": parsed["release_endpoint_lines"],

        "release_creations": int(counts["release_creation"]),
        "release_updates": int(counts["release_update"]),
        "release_info_views": int(counts["release_info_view"]),
        "release_endpoint_non_200": int(counts["release_endpoint_non_200"]),
        "release_endpoint_other_200": int(counts["release_endpoint_other_200"]),

        "metric_name": "repository_release_activity",
        "metric_group": "operational_effectiveness",
        "calculation": (
            "release_creations = POST 200 /lca-collaboration/ws/release/...; "
            "release_updates = PUT 200; release_info_views = GET 200"
        ),
        "metric_note": (
            "Corrected release KPI logic. git-receive-pack measures repository push/update activity, "
            "but actual release/publish activity is captured by the application release endpoint."
        ),
    }

    print("\n=== RELEASE KPI SUMMARY ===")
    print(f"Window: {summary_doc['window_start']} to {summary_doc['window_end']}")
    print(f"Total log lines: {summary_doc['total_lines']:,}")
    print(f"Release endpoint lines: {summary_doc['release_endpoint_lines']:,}")
    print(f"Release creations: {summary_doc['release_creations']}")
    print(f"Release edits/updates: {summary_doc['release_updates']}")
    print(f"Release info views: {summary_doc['release_info_views']}")
    print(f"Release endpoint non-200: {summary_doc['release_endpoint_non_200']}")

    create_summary_index(args.es_url, args.summary_index)
    index_summary(args.es_url, args.summary_index, summary_doc, args.doc_id)

    if not args.skip_events:
        create_events_index(args.es_url, args.events_index)
        bulk_index_events(args.es_url, args.events_index, parsed["events"])

    print("\nDone.")


if __name__ == "__main__":
    main()
