#!/usr/bin/env python3

"""
LCACS Estimated Process Downloads KPI

Builds an event-level derived KPI index from successful public repository
``download_prepare`` events. Each output document preserves the original event
``@timestamp`` and request metadata, then adds the repository process-count
multipliers used by the estimated-process-download KPI.

Because the output remains event-level, Kibana can correctly aggregate any
arbitrary date range while retaining client IP, request, user agent, status,
and other source-event fields.
"""

SCRIPT_NAME = "index_estimated_process_downloads"
SCRIPT_VERSION = "2.0.0"

import argparse
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional, Tuple

import requests

DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_DOWNLOAD_INDEX = "lcacs-kpi-public-repo-downloads"
DEFAULT_INVENTORY_INDEX = "lcacs-kpi-public-process-inventory-v1"
DEFAULT_TARGET_INDEX = "lcacs-kpi-estimated-process-downloads"
LCACS_BROWSE_BASE = "https://www.lcacommons.gov/lca-collaboration/ws/public/browse"

SOURCE_FIELDS = [
    "@timestamp", "host", "client_ip", "method", "request", "endpoint",
    "status", "bytes", "referrer", "user_agent", "upstream", "request_time",
    "download_event_type", "group", "repo", "repo_path", "commit_id",
    "is_public_repo", "is_repo_identifiable",
]


def es_request(method: str, es_url: str, path: str, payload: Optional[dict] = None,
               timeout: int = 120) -> dict:
    url = f"{es_url.rstrip('/')}/{path.lstrip('/')}"
    kwargs: Dict[str, Any] = {"timeout": timeout}
    if payload is not None:
        kwargs["json"] = payload
    resp = requests.request(method, url, **kwargs)
    if not resp.ok:
        raise RuntimeError(f"{method} {url} failed: {resp.status_code} {resp.text}")
    return resp.json() if resp.text else {}


def create_target_index(es_url: str, index: str, recreate: bool = False) -> None:
    mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "source_event_id": {"type": "keyword"},
                "source_event_index": {"type": "keyword"},
                "host": {"type": "keyword"},
                "client_ip": {"type": "ip"},
                "method": {"type": "keyword"},
                "request": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 4096}}},
                "endpoint": {"type": "keyword"},
                "status": {"type": "integer"},
                "bytes": {"type": "long"},
                "referrer": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 4096}}},
                "user_agent": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 4096}}},
                "upstream": {"type": "keyword"},
                "request_time": {"type": "float"},
                "download_event_type": {"type": "keyword"},
                "repo_path": {"type": "keyword"},
                "group": {"type": "keyword"},
                "repo": {"type": "keyword"},
                "commit_id": {"type": "keyword"},
                "is_public_repo": {"type": "boolean"},
                "is_repo_identifiable": {"type": "boolean"},
                "completed_public_repo_downloads": {"type": "long"},
                "current_unit_process_count": {"type": "long"},
                "current_lci_result_count": {"type": "long"},
                "current_total_process_count": {"type": "long"},
                "estimated_unit_process_downloads": {"type": "long"},
                "estimated_lci_result_downloads": {"type": "long"},
                "estimated_total_process_downloads": {"type": "long"},
                "unit_process_count_source": {"type": "keyword"},
                "total_process_count_source": {"type": "keyword"},
                "lci_result_count_source": {"type": "keyword"},
                "process_count_status": {"type": "keyword"},
                "process_count_note": {"type": "text"},
                "download_count_source": {"type": "keyword"},
                "source": {"type": "keyword"},
                "kpi_name": {"type": "keyword"},
                "script_name": {"type": "keyword"},
                "script_version": {"type": "keyword"},
                "run_label": {"type": "keyword"},
                "kpi_period_start": {"type": "date"},
                "kpi_period_end": {"type": "date"},
                "generated_at": {"type": "date"},
            }
        },
    }
    if recreate:
        requests.delete(f"{es_url.rstrip('/')}/{index}", timeout=60)
    if requests.head(f"{es_url.rstrip('/')}/{index}", timeout=30).status_code == 200:
        print(f"Index already exists: {index}")
        return
    es_request("PUT", es_url, index, mapping)
    print(f"Created index: {index}")


def get_inventory(es_url: str, inventory_index: str) -> Dict[str, Dict[str, Any]]:
    payload = {
        "size": 1000,
        "_source": [
            "repo_path", "group", "repo", "commit_id",
            "current_unit_process_count", "current_lci_result_count",
            "current_total_process_count", "process_count_status",
            "process_count_note", "unit_process_count_source",
            "total_process_count_source", "lci_result_count_source",
        ],
        "query": {"match_all": {}},
    }
    data = es_request("GET", es_url, f"{inventory_index}/_search", payload)
    return {
        hit["_source"]["repo_path"]: hit["_source"]
        for hit in data.get("hits", {}).get("hits", [])
        if hit.get("_source", {}).get("repo_path")
    }


def get_total_process_count_from_browse(repo_path: str) -> Dict[str, Any]:
    if "/" not in repo_path:
        return {"current_total_process_count": 0, "commit_id": None,
                "status": "bad_repo_path", "note": "Invalid repo_path."}
    group, repo = repo_path.split("/", 1)
    try:
        resp = requests.get(f"{LCACS_BROWSE_BASE}/{group}/{repo}", timeout=120)
        resp.raise_for_status()
        for entry in resp.json().get("data", []):
            if entry.get("type") == "PROCESS" and entry.get("typeOfEntry") == "MODEL_TYPE":
                return {"current_total_process_count": int(entry.get("count", 0) or 0),
                        "commit_id": entry.get("commitId"), "status": "available",
                        "note": "PROCESS total from public browse endpoint."}
        return {"current_total_process_count": 0, "commit_id": None,
                "status": "missing_process_entry", "note": "No PROCESS entry in browse response."}
    except Exception as exc:
        return {"current_total_process_count": 0, "commit_id": None,
                "status": "api_error", "note": f"Browse lookup failed: {exc}"}


def build_enrichment(repo_path: str, inventory: Dict[str, Dict[str, Any]],
                     inventory_index: str) -> Tuple[Dict[str, Any], Optional[str]]:
    inv = inventory.get(repo_path, {})
    unit_count = int(inv.get("current_unit_process_count", 0) or 0)
    lci_count = int(inv.get("current_lci_result_count", 0) or 0)
    total_count = int(inv.get("current_total_process_count", 0) or 0)
    commit_id = inv.get("commit_id")
    status = inv.get("process_count_status")
    note = inv.get("process_count_note")

    if total_count <= 0:
        browse = get_total_process_count_from_browse(repo_path)
        total_count = int(browse.get("current_total_process_count", 0) or 0)
        commit_id = commit_id or browse.get("commit_id")
        if total_count > 0 and unit_count > 0 and total_count >= unit_count:
            lci_count = total_count - unit_count
            status = "available"
            note = "PROCESS total from browse; UNIT_PROCESS from inventory; LCI_RESULT derived."
        elif total_count > 0 and unit_count <= 0:
            unit_count = total_count
            lci_count = 0
            status = "inferred_unit_process_count_from_public_process_total"
            note = "No validated split; PROCESS total treated as UNIT_PROCESS."
        else:
            total_count = unit_count
            lci_count = 0
            status = "fallback_unit_only"
            note = f"{browse.get('note', '')} Falling back to UNIT_PROCESS count."

    enrichment = {
        "commit_id": commit_id,
        "current_unit_process_count": unit_count,
        "current_lci_result_count": lci_count,
        "current_total_process_count": total_count,
        "unit_process_count_source": inv.get("unit_process_count_source", inventory_index if inv else "missing"),
        "total_process_count_source": inv.get("total_process_count_source", "inventory_or_browse"),
        "lci_result_count_source": inv.get("lci_result_count_source", "inventory_or_derived"),
        "process_count_status": status or "unknown",
        "process_count_note": note or "",
    }
    warning = None if enrichment["process_count_status"] == "available" else f"{repo_path}: {enrichment['process_count_status']}"
    return enrichment, warning


def iter_download_events(es_url: str, download_index: str, start_date: Optional[str],
                         end_date: Optional[str], page_size: int = 5000) -> Iterator[dict]:
    filters = [
        {"term": {"download_event_type": "download_prepare"}},
        {"range": {"status": {"gte": 200, "lt": 300}}},
    ]
    if start_date or end_date:
        bounds: Dict[str, str] = {}
        if start_date:
            bounds["gte"] = f"{start_date}T00:00:00Z"
        if end_date:
            bounds["lt"] = f"{end_date}T00:00:00Z"
        filters.append({"range": {"@timestamp": bounds}})

    payload = {
        "size": page_size,
        "_source": SOURCE_FIELDS,
        "query": {"bool": {"filter": filters}},
        "sort": ["_doc"],
    }
    data = es_request("POST", es_url, f"{download_index}/_search?scroll=2m", payload)
    scroll_id = data.get("_scroll_id")
    try:
        while True:
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break
            yield from hits
            data = es_request("POST", es_url, "_search/scroll", {"scroll": "2m", "scroll_id": scroll_id})
            scroll_id = data.get("_scroll_id", scroll_id)
    finally:
        if scroll_id:
            try:
                es_request("DELETE", es_url, "_search/scroll", {"scroll_id": [scroll_id]})
            except Exception:
                pass


def bulk_index(es_url: str, target_index: str, actions: list) -> None:
    if not actions:
        return
    payload = "\n".join(json.dumps(item, separators=(",", ":")) for item in actions) + "\n"
    resp = requests.post(f"{es_url.rstrip('/')}/_bulk", data=payload,
                         headers={"Content-Type": "application/x-ndjson"}, timeout=180)
    if not resp.ok:
        raise RuntimeError(f"Bulk failed: {resp.status_code} {resp.text}")
    result = resp.json()
    if result.get("errors"):
        errors = [x for x in result.get("items", []) if x.get("index", {}).get("error")]
        raise RuntimeError(f"Bulk contained errors: {json.dumps(errors[:5])}")


def run(es_url: str, download_index: str, inventory_index: str, target_index: str,
        start_date: Optional[str], end_date: Optional[str], run_label: str,
        dry_run: bool, batch_size: int = 5000) -> None:
    inventory = get_inventory(es_url, inventory_index)
    enrichment_cache: Dict[str, Dict[str, Any]] = {}
    warnings = set()
    generated_at = datetime.now(timezone.utc).isoformat()
    actions = []
    events = 0
    estimated_total = 0

    for hit in iter_download_events(es_url, download_index, start_date, end_date):
        src = hit.get("_source", {})
        repo_path = src.get("repo_path")
        if not repo_path:
            continue
        if repo_path not in enrichment_cache:
            enrichment, warning = build_enrichment(repo_path, inventory, inventory_index)
            enrichment_cache[repo_path] = enrichment
            if warning:
                warnings.add(warning)
        enrichment = enrichment_cache[repo_path]

        unit = int(enrichment["current_unit_process_count"])
        lci = int(enrichment["current_lci_result_count"])
        total = int(enrichment["current_total_process_count"])
        doc = dict(src)
        doc.update(enrichment)
        doc.update({
            "source_event_id": hit.get("_id"),
            "source_event_index": hit.get("_index"),
            "completed_public_repo_downloads": 1,
            "estimated_unit_process_downloads": unit,
            "estimated_lci_result_downloads": lci,
            "estimated_total_process_downloads": total,
            "download_count_source": download_index,
            "source": "lcacs_kpi_event_enrichment",
            "kpi_name": "estimated_process_downloads",
            "script_name": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "run_label": run_label,
            "kpi_period_start": start_date,
            "kpi_period_end": end_date,
            "generated_at": generated_at,
        })
        raw_id = f"{hit.get('_index')}|{hit.get('_id')}|{SCRIPT_VERSION}"
        doc_id = hashlib.sha1(raw_id.encode("utf-8")).hexdigest()
        actions.extend([{"index": {"_index": target_index, "_id": doc_id}}, doc])
        events += 1
        estimated_total += total

        if len(actions) >= batch_size * 2:
            if not dry_run:
                bulk_index(es_url, target_index, actions)
            actions.clear()

    if actions and not dry_run:
        bulk_index(es_url, target_index, actions)

    print("\n=== ESTIMATED PROCESS DOWNLOADS EVENT-LEVEL SUMMARY ===")
    print(f"Source events indexed: {events:,}")
    print(f"Repositories enriched: {len(enrichment_cache):,}")
    print(f"Estimated total process downloads: {estimated_total:,}")
    print(f"KPI period: {start_date} to {end_date} (end exclusive)")
    print(f"Dry run: {dry_run}")
    if warnings:
        print("Warnings:")
        for warning in sorted(warnings):
            print(f"  {warning}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build event-level estimated process-download KPI index.")
    parser.add_argument("--es-url", default=DEFAULT_ES_URL)
    parser.add_argument("--download-index", default=DEFAULT_DOWNLOAD_INDEX)
    parser.add_argument("--inventory-index", "--old-unit-index", dest="inventory_index", default=DEFAULT_INVENTORY_INDEX)
    parser.add_argument("--target-index", "--index", dest="target_index", default=DEFAULT_TARGET_INDEX)
    parser.add_argument("--start-date", help="Inclusive start date, YYYY-MM-DD")
    parser.add_argument("--end-date", help="Exclusive end date, YYYY-MM-DD")
    parser.add_argument("--run-label", default="manual")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    create_target_index(args.es_url, args.target_index, recreate=args.recreate)
    run(args.es_url, args.download_index, args.inventory_index, args.target_index,
        args.start_date, args.end_date, args.run_label, args.dry_run, args.batch_size)


if __name__ == "__main__":
    main()
