#!/usr/bin/env python3

"""
LCACS Total Repositories Published KPI

Canonical implementation.

This script uses git-receive-pack activity from LCACS web server logs as a
repository publication proxy and produces the OpenSearch index used by the
Total Repositories Published KPI dashboard.

Designed to be executed directly or orchestrated by the KPI framework.
Runtime parameters are supplied via command-line arguments.
"""

SCRIPT_NAME = "index_total_repositories_published"
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
DEFAULT_INDEX = "lcacs-kpi-total-repositories-published"

TS_FORMAT = "%d/%b/%Y:%H:%M:%S %z"

LOG_RE = re.compile(
    r'^(?P<host>\S+)\s+'
    r'(?P<client_ip>\S+)\s+\S+\s+(?P<user>\S+)\s+'
    r'\[(?P<timestamp>[^\]]+)\]\s+'
    r'"(?P<method>[A-Z]+)\s+(?P<request>\S+)\s+HTTP/[0-9.]+"\s+'
    r'(?P<status>\d{3})\s+'
    r'(?P<bytes>\S+)\s+'
    r'"(?P<referrer>[^"]*)"\s+'
    r'"(?P<user_agent>[^"]*)"\s+'
    r'"(?P<upstream>[^"]*)"\s+'
    r'(?P<request_time>\S+)'
)


def open_log(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", errors="replace")
    return path.open("rt", errors="replace")


def parse_timestamp(raw: str) -> str:
    dt = datetime.strptime(raw, TS_FORMAT)
    return dt.astimezone(timezone.utc).isoformat()


def normalize_endpoint(request: str) -> str:
    request = request.strip()

    if request.startswith("http://") or request.startswith("https://"):
        after_scheme = request.split("://", 1)[1]
        slash_pos = after_scheme.find("/")
        request = "/" + after_scheme[slash_pos + 1:] if slash_pos >= 0 else "/"

    request = request.split("?", 1)[0]
    request = request.split("#", 1)[0]

    try:
        return unquote(request)
    except Exception:
        return request


def parse_git_receive_pack_endpoint(endpoint: str):
    """
    Expected:
      /lca-collaboration/{group}/{repo}/git-receive-pack

    Excludes:
      /info/refs discovery calls
    """

    if "info/refs" in endpoint:
        return None

    prefix = "/lca-collaboration/"
    suffix = "/git-receive-pack"

    if not endpoint.startswith(prefix):
        return None

    if not endpoint.endswith(suffix):
        return None

    middle = endpoint[len(prefix):-len(suffix)]
    parts = middle.split("/")

    if len(parts) < 2:
        return None

    group = parts[0]
    repo = parts[1]

    return {
        "group": group,
        "repo": repo,
        "repo_path": f"{group}/{repo}",
    }


def create_index(es_url: str, index: str, recreate: bool = False):
    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},

                "first_observed_publish_timestamp": {"type": "date"},
                "last_observed_publish_timestamp": {"type": "date"},

                "host": {"type": "keyword"},
                "client_ip": {"type": "ip"},
                "user": {"type": "keyword"},
                "method": {"type": "keyword"},
                "status": {"type": "integer"},

                "request": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 4096}
                    },
                },
                "endpoint": {"type": "keyword"},

                "group": {"type": "keyword"},
                "repo": {"type": "keyword"},
                "repo_path": {"type": "keyword"},

                "push_count": {"type": "long"},
                "first_client_ip": {"type": "ip"},
                "first_user": {"type": "keyword"},
                "first_user_agent": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 4096}
                    },
                },

                "source": {"type": "keyword"},
                "kpi_name": {"type": "keyword"},
                "kpi_definition": {"type": "text"},

                # Framework metadata
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
        requests.delete(f"{es_url}/{index}", timeout=60)

    exists = requests.head(f"{es_url}/{index}", timeout=30).status_code == 200
    if exists:
        print(f"Index already exists: {index}")
        return

    resp = requests.put(f"{es_url}/{index}", json=mapping, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Create index failed: {resp.status_code} {resp.text}")

    print(f"Created index: {index}")


def bulk_index(es_url: str, index: str, docs: list):
    if not docs:
        return

    lines = []

    for doc in docs:
        doc_id = hashlib.sha1(doc["repo_path"].encode("utf-8")).hexdigest()
        lines.append({"index": {"_index": index, "_id": doc_id}})
        lines.append(doc)

    payload = "\n".join(json.dumps(x) for x in lines) + "\n"

    resp = requests.post(
        f"{es_url}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data=payload,
        timeout=120,
    )

    if not resp.ok:
        raise RuntimeError(f"Bulk failed: {resp.status_code} {resp.text}")

    result = resp.json()
    if result.get("errors"):
        raise RuntimeError(f"Bulk had errors: {json.dumps(result)[:3000]}")


def parse_log_file(
    log_file: Path,
    es_url: str,
    index: str,
    start_date: str = None,
    end_date: str = None,
    run_label: str = "manual",
    dry_run: bool = False,
):
    total_lines = 0
    matching_push_rows = 0
    successful_push_rows = 0

    by_repo = {}
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

            if "git-receive-pack" not in endpoint:
                continue

            matching_push_rows += 1

            method = row["method"]
            status = int(row["status"])

            if method != "POST":
                continue

            if status != 200:
                continue

            parsed = parse_git_receive_pack_endpoint(endpoint)
            if not parsed:
                continue

            successful_push_rows += 1

            ts = parse_timestamp(row["timestamp"])
            repo_path = parsed["repo_path"]

            if repo_path not in by_repo:
                by_repo[repo_path] = {
                    "@timestamp": ts,
                    "first_observed_publish_timestamp": ts,
                    "last_observed_publish_timestamp": ts,

                    "host": row["host"],
                    "client_ip": row["client_ip"],
                    "user": row["user"],
                    "method": method,
                    "status": status,

                    "request": request,
                    "endpoint": endpoint,

                    "group": parsed["group"],
                    "repo": parsed["repo"],
                    "repo_path": repo_path,

                    "push_count": 1,
                    "first_client_ip": row["client_ip"],
                    "first_user": row["user"],
                    "first_user_agent": row["user_agent"],

                    "source": "lcacs_web_logs",
                    "kpi_name": "total_repositories_published",
                    "kpi_definition": (
                        "First observed successful POST git-receive-pack event "
                        "with HTTP 200 for a repository in retained web logs."
                    ),

                    # Framework metadata
                    "script_name": SCRIPT_NAME,
                    "script_version": SCRIPT_VERSION,
                    "run_label": run_label,
                    "kpi_period_start": start_date,
                    "kpi_period_end": end_date,
                    "generated_at": generated_at,
                }
            else:
                existing = by_repo[repo_path]
                existing["push_count"] += 1

                if ts < existing["first_observed_publish_timestamp"]:
                    existing["@timestamp"] = ts
                    existing["first_observed_publish_timestamp"] = ts
                    existing["host"] = row["host"]
                    existing["client_ip"] = row["client_ip"]
                    existing["user"] = row["user"]
                    existing["request"] = request
                    existing["endpoint"] = endpoint
                    existing["first_client_ip"] = row["client_ip"]
                    existing["first_user"] = row["user"]
                    existing["first_user_agent"] = row["user_agent"]

                if ts > existing["last_observed_publish_timestamp"]:
                    existing["last_observed_publish_timestamp"] = ts

    docs = list(by_repo.values())

    if not dry_run:
        bulk_index(es_url, index, docs)
    else:
        print("Dry run enabled; skipping bulk index.")

    print("\n=== TOTAL REPOSITORIES PUBLISHED SUMMARY ===")
    print(f"Script: {SCRIPT_NAME} {SCRIPT_VERSION}")
    print(f"Run label: {run_label}")
    print(f"KPI period: {start_date} to {end_date}")
    print(f"Dry run: {dry_run}")

    print(f"Total raw log lines read: {total_lines:,}")
    print(f"git-receive-pack rows seen: {matching_push_rows:,}")
    print(f"Successful POST git-receive-pack 200 rows: {successful_push_rows:,}")
    print(f"Unique repositories with successful write/sync event: {len(docs):,}")

    print("\nRepositories:")
    for d in sorted(docs, key=lambda x: x["first_observed_publish_timestamp"]):
        print(
            f"  {d['first_observed_publish_timestamp']}\t"
            f"{d['repo_path']}\t"
            f"push_count={d['push_count']}\t"
            f"user={d['first_user']}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Index first observed successful git-receive-pack POST 200 events by repository."
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
