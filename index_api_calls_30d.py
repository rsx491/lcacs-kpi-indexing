#!/usr/bin/env python3

import argparse
import gzip
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests


# =========================
# Config defaults
# =========================

DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_INDEX = "lcacs-kpi-api-calls-30d"

# Example log line:
# arsgcazu0ws700.nal.usda.gov 1.156.43.167 - - [01/May/2026:22:13:52 -0400] "GET /lca-collaboration/ws/public/search?page=1&pageSize=2&query=paper+cup&api_key=... HTTP/1.1" 200 11487 "-" "python-requests/2.32.5" "1.156.43.167:65490" 0.451

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


def create_index(es_url: str, index: str, recreate: bool = False):
    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "host": {"type": "keyword"},
                "client_ip": {"type": "ip"},
                "method": {"type": "keyword"},
                "request": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 2048}}},
                "endpoint": {"type": "keyword"},
                "status": {"type": "integer"},
                "bytes": {"type": "long"},
                "referrer": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 2048}}},
                "user_agent": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 2048}}},
                "upstream": {"type": "keyword"},
                "request_time": {"type": "float"},

                "is_public_api_call": {"type": "boolean"},
                "has_api_key": {"type": "boolean"},
                "api_auth_type": {"type": "keyword"},   # token or non_token
                "api_key_hash": {"type": "keyword"},

                "source": {"type": "keyword"},
                "kpi_name": {"type": "keyword"},
            }
        }
    }

    if recreate:
        requests.delete(f"{es_url}/{index}", timeout=30)

    exists = requests.head(f"{es_url}/{index}", timeout=30).status_code == 200
    if not exists:
        resp = requests.put(f"{es_url}/{index}", json=mapping, timeout=30)
        if not resp.ok:
            raise RuntimeError(f"Create index failed: {resp.status_code} {resp.text}")
        print(f"Created index: {index}")
    else:
        print(f"Index already exists: {index}")


def bulk_index(es_url: str, index: str, docs: list):
    if not docs:
        return

    lines = []
    for doc in docs:
        # deterministic-ish ID to avoid duplicate docs on rerun
        raw_id = f"{doc['@timestamp']}|{doc['client_ip']}|{doc['method']}|{doc['request']}|{doc['status']}"
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
        raise RuntimeError(f"Bulk had errors: {json.dumps(result)[:2000]}")


def parse_log_file(log_file: Path, es_url: str, index: str, batch_size: int = 5000):
    total_lines = 0
    parsed_lines = 0
    public_api_calls = 0

    token_calls = 0
    token_success = 0
    non_token_calls = 0
    non_token_success = 0

    status_counts = {}
    endpoint_counts = {}

    batch = []

    with open_log(log_file) as f:
        for line in f:
            total_lines += 1

            m = LOG_RE.match(line)
            if not m:
                continue

            row = m.groupdict()
            request = row["request"]

            # Only API/public web-service calls for this dashboard
            if "/lca-collaboration/ws/public/" not in request:
                continue

            parsed_lines += 1
            public_api_calls += 1

            endpoint = normalize_endpoint(request)
            parsed = urlparse(request)
            qs = parse_qs(parsed.query)

            api_key = None
            if "api_key" in qs and qs["api_key"]:
                api_key = qs["api_key"][0]

            has_api_key = api_key is not None and api_key != ""
            api_auth_type = "token" if has_api_key else "non_token"

            status = int(row["status"])
            is_success = 200 <= status < 300

            if has_api_key:
                token_calls += 1
                if is_success:
                    token_success += 1
            else:
                non_token_calls += 1
                if is_success:
                    non_token_success += 1

            status_counts[status] = status_counts.get(status, 0) + 1
            endpoint_counts[endpoint] = endpoint_counts.get(endpoint, 0) + 1

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

                "is_public_api_call": True,
                "has_api_key": has_api_key,
                "api_auth_type": api_auth_type,
                "api_key_hash": hash_api_key(api_key) if has_api_key else None,

                "source": "lcacs_web_logs_30d",
                "kpi_name": "api_calls_token_vs_non_token",
            }

            batch.append(doc)

            if len(batch) >= batch_size:
                bulk_index(es_url, index, batch)
                batch.clear()

    if batch:
        bulk_index(es_url, index, batch)

    print("\n=== API CALL INDEXING SUMMARY ===")
    print(f"Total raw log lines read: {total_lines:,}")
    print(f"Public /ws/public API calls indexed: {public_api_calls:,}")
    print(f"Token API calls has api_key=: {token_calls:,}")
    print(f"Successful token API calls 2xx: {token_success:,}")
    print(f"Non-token API calls: {non_token_calls:,}")
    print(f"Successful non-token API calls 2xx: {non_token_success:,}")

    print("\nStatus counts:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count:,}")

    print("\nTop endpoints:")
    for endpoint, count in sorted(endpoint_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"  {count:,}\t{endpoint}")


def main():
    parser = argparse.ArgumentParser(description="Index LCACS 30-day public API calls token vs non-token.")
    parser.add_argument("log_file", help="Path to logs_30d_all.log or logs_30d_all.log.gz")
    parser.add_argument("--es-url", default=DEFAULT_ES_URL)
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--recreate", action="store_true", help="Delete and recreate target index first.")
    args = parser.parse_args()

    log_file = Path(args.log_file)
    if not log_file.exists():
        raise SystemExit(f"File not found: {log_file}")

    create_index(args.es_url, args.index, recreate=args.recreate)
    parse_log_file(log_file, args.es_url, args.index)


if __name__ == "__main__":
    main()
