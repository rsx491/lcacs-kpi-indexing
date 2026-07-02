#!/usr/bin/env python3

import argparse
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional

import requests


DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_DOWNLOAD_INDEX = "lcacs-kpi-public-repo-downloads-30d-v1"
DEFAULT_TARGET_INDEX = "lcacs-kpi-estimated-process-downloads-30d-v3"

LCACS_SEARCH_URL = "https://www.lcacommons.gov/lca-collaboration/ws/public/search"


def es_request(method: str, es_url: str, path: str, payload: Optional[dict] = None) -> dict:
    url = f"{es_url}/{path.lstrip('/')}"
    kwargs = {"timeout": 120}

    if payload is not None:
        kwargs["headers"] = {"Content-Type": "application/json"}
        kwargs["data"] = json.dumps(payload)

    resp = requests.request(method, url, **kwargs)

    if not resp.ok:
        raise RuntimeError(f"{method} {url} failed: {resp.status_code} {resp.text}")

    return resp.json() if resp.text else {}


def create_target_index(es_url: str, index: str, recreate: bool = False):
    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},

                "repo_path": {"type": "keyword"},
                "group": {"type": "keyword"},
                "repo": {"type": "keyword"},

                "completed_public_repo_downloads": {"type": "long"},

                "current_unit_process_count": {"type": "long"},
                "current_lci_result_count": {"type": "long"},
                "current_total_process_count": {"type": "long"},

                "estimated_unit_process_downloads": {"type": "long"},
                "estimated_lci_result_downloads": {"type": "long"},
                "estimated_total_process_downloads": {"type": "long"},

                "process_count_status": {"type": "keyword"},
                "process_count_source": {"type": "keyword"},
                "process_count_note": {"type": "text"},

                "download_count_source": {"type": "keyword"},
                "source": {"type": "keyword"},
                "kpi_name": {"type": "keyword"},
            }
        }
    }

    if recreate:
        requests.delete(f"{es_url}/{index}", timeout=60)

    exists = requests.head(f"{es_url}/{index}", timeout=30).status_code == 200
    if exists:
        print(f"Index already exists: {index}")
        return

    es_request("PUT", es_url, index, mapping)
    print(f"Created index: {index}")


def get_public_repo_download_counts(es_url: str, download_index: str) -> Dict[str, int]:
    """
    KPI download definition:
      Successful public repository download event =
      download_event_type=download_prepare AND status 2xx

    This avoids double-counting the prepare + actual download two-step workflow.
    """

    payload = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"download_event_type": "download_prepare"}},
                    {"range": {"status": {"gte": 200, "lt": 300}}},
                ]
            }
        },
        "aggs": {
            "by_repo": {
                "terms": {
                    "field": "repo_path",
                    "size": 1000,
                    "order": {"_count": "desc"},
                }
            }
        },
    }

    data = es_request("GET", es_url, f"{download_index}/_search", payload)

    counts = {}
    buckets = data.get("aggregations", {}).get("by_repo", {}).get("buckets", [])

    for bucket in buckets:
        counts[bucket["key"]] = int(bucket["doc_count"])

    return counts


def call_lcacs_process_search(group: str, repo_path: str) -> dict:
    """
    Attempt repo-specific LCACS process aggregation.

    The important validation is downstream:
    - repositoryId aggregation must include the requested repo
    - ideally it should return only one repositoryId bucket
    - processType aggregation should contain UNIT_PROCESS and/or LCI_RESULT
    """

    params = {
        "group": group,
        "repositoryId": repo_path,
        "type": "PROCESS",
        "page": 1,
        "pageSize": 1,
    }

    resp = requests.get(LCACS_SEARCH_URL, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


def extract_process_counts(api_data: dict, repo_path: str) -> Tuple[int, int, str, str]:
    """
    Returns:
      unit_count, lci_result_count, status, note

    LCI_RESULT maps to System Process.
    UNIT_PROCESS maps to Unit Process.
    """

    repository_entries = []
    process_type_entries = []

    for agg in api_data.get("aggregations", []):
        name = agg.get("name")

        if name == "repositoryId":
            repository_entries = agg.get("entries", []) or []

        if name == "processType":
            process_type_entries = agg.get("entries", []) or []

    repo_keys = [entry.get("key") for entry in repository_entries]

    if repo_path not in repo_keys:
        return (
            0,
            0,
            "needs_validation",
            "Requested repository was not present in repositoryId aggregation; process counts were not safely assigned.",
        )

    if len(repository_entries) > 1:
        return (
            0,
            0,
            "needs_validation",
            "API response contained multiple repositoryId buckets, so processType aggregation may be group-level rather than repo-specific.",
        )

    unit_count = 0
    lci_result_count = 0

    for entry in process_type_entries:
        key = entry.get("key")
        count = int(entry.get("count", 0) or 0)

        if key == "UNIT_PROCESS":
            unit_count = count

        if key == "LCI_RESULT":
            lci_result_count = count

    return (
        unit_count,
        lci_result_count,
        "available",
        "Counts pulled from LCACS public search processType aggregation. UNIT_PROCESS=Unit Process; LCI_RESULT=System Process.",
    )


def get_process_counts_for_repo(repo_path: str, api_cache: dict) -> dict:
    if "/" in repo_path:
        group, repo = repo_path.split("/", 1)
    else:
        group, repo = None, repo_path

    if not group:
        return {
            "repo_path": repo_path,
            "group": group,
            "repo": repo,
            "current_unit_process_count": 0,
            "current_lci_result_count": 0,
            "current_total_process_count": 0,
            "process_count_status": "needs_validation",
            "process_count_source": "none",
            "process_count_note": "Could not split repo_path into group/repo.",
        }

    if repo_path not in api_cache:
        try:
            api_data = call_lcacs_process_search(group, repo_path)
            unit_count, lci_count, status, note = extract_process_counts(api_data, repo_path)
            api_cache[repo_path] = {
                "repo_path": repo_path,
                "group": group,
                "repo": repo,
                "current_unit_process_count": unit_count,
                "current_lci_result_count": lci_count,
                "current_total_process_count": unit_count + lci_count,
                "process_count_status": status,
                "process_count_source": "lcacs_public_search_api",
                "process_count_note": note,
            }
        except Exception as e:
            api_cache[repo_path] = {
                "repo_path": repo_path,
                "group": group,
                "repo": repo,
                "current_unit_process_count": 0,
                "current_lci_result_count": 0,
                "current_total_process_count": 0,
                "process_count_status": "api_error",
                "process_count_source": "lcacs_public_search_api",
                "process_count_note": f"API lookup failed: {e}",
            }

    return api_cache[repo_path]


def bulk_index(es_url: str, index: str, docs: list):
    if not docs:
        print("No docs to index.")
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


def build_docs(download_counts: Dict[str, int], download_index: str) -> Tuple[list, list]:
    docs = []
    warnings = []
    api_cache = {}
    now = datetime.now(timezone.utc).isoformat()

    for repo_path, downloads in sorted(download_counts.items(), key=lambda x: x[1], reverse=True):
        process = get_process_counts_for_repo(repo_path, api_cache)

        unit_count = int(process["current_unit_process_count"])
        lci_count = int(process["current_lci_result_count"])
        total_count = int(process["current_total_process_count"])

        doc = {
            "@timestamp": now,

            "repo_path": repo_path,
            "group": process.get("group"),
            "repo": process.get("repo"),

            "completed_public_repo_downloads": int(downloads),

            "current_unit_process_count": unit_count,
            "current_lci_result_count": lci_count,
            "current_total_process_count": total_count,

            "estimated_unit_process_downloads": int(downloads) * unit_count,
            "estimated_lci_result_downloads": int(downloads) * lci_count,
            "estimated_total_process_downloads": int(downloads) * total_count,

            "process_count_status": process.get("process_count_status"),
            "process_count_source": process.get("process_count_source"),
            "process_count_note": process.get("process_count_note"),

            "download_count_source": download_index,
            "source": "lcacs_kpi_enrichment",
            "kpi_name": "estimated_process_downloads",
        }

        if doc["process_count_status"] != "available":
            warnings.append(
                f"{repo_path}: {doc['process_count_status']} - {doc['process_count_note']}"
            )

        docs.append(doc)

    return docs, warnings


def print_summary(docs: list, warnings: list):
    total_downloads = sum(d["completed_public_repo_downloads"] for d in docs)

    total_unit_processes = sum(d["current_unit_process_count"] for d in docs)
    total_lci_results = sum(d["current_lci_result_count"] for d in docs)
    total_processes = sum(d["current_total_process_count"] for d in docs)

    estimated_unit = sum(d["estimated_unit_process_downloads"] for d in docs)
    estimated_lci = sum(d["estimated_lci_result_downloads"] for d in docs)
    estimated_total = sum(d["estimated_total_process_downloads"] for d in docs)

    available = sum(1 for d in docs if d["process_count_status"] == "available")
    needs_validation = len(docs) - available

    print("\n=== ESTIMATED PROCESS DOWNLOADS SUMMARY ===")
    print(f"Repositories indexed: {len(docs):,}")
    print(f"Repositories with available process counts: {available:,}")
    print(f"Repositories needing validation/API issue: {needs_validation:,}")

    print(f"Completed public repo downloads: {total_downloads:,}")

    print(f"Current UNIT_PROCESS count across downloaded repos: {total_unit_processes:,}")
    print(f"Current LCI_RESULT/System Process count across downloaded repos: {total_lci_results:,}")
    print(f"Current total process count across downloaded repos: {total_processes:,}")

    print(f"Estimated UNIT_PROCESS downloads: {estimated_unit:,}")
    print(f"Estimated LCI_RESULT/System Process downloads: {estimated_lci:,}")
    print(f"Estimated total process downloads: {estimated_total:,}")

    print("\nTop 20 repos by estimated total process downloads:")
    for d in sorted(docs, key=lambda x: x["estimated_total_process_downloads"], reverse=True)[:20]:
        print(
            f"  {d['estimated_total_process_downloads']:,}\t"
            f"{d['repo_path']}\t"
            f"downloads={d['completed_public_repo_downloads']:,}\t"
            f"unit={d['current_unit_process_count']:,}\t"
            f"lci={d['current_lci_result_count']:,}\t"
            f"total={d['current_total_process_count']:,}\t"
            f"status={d['process_count_status']}"
        )

    if warnings:
        print("\nWARNINGS:")
        for warning in warnings:
            print(f"  {warning}")


def main():
    parser = argparse.ArgumentParser(
        description="Create estimated process-download KPI index using public downloads plus UNIT_PROCESS and LCI_RESULT counts."
    )

    parser.add_argument("--es-url", default=DEFAULT_ES_URL)
    parser.add_argument("--download-index", default=DEFAULT_DOWNLOAD_INDEX)
    parser.add_argument("--target-index", default=DEFAULT_TARGET_INDEX)
    parser.add_argument("--recreate", action="store_true")

    args = parser.parse_args()

    create_target_index(args.es_url, args.target_index, recreate=args.recreate)

    print(f"Reading download counts from: {args.download_index}")
    download_counts = get_public_repo_download_counts(args.es_url, args.download_index)
    print(f"Found public repo download counts for {len(download_counts):,} repos.")

    docs, warnings = build_docs(download_counts, args.download_index)

    bulk_index(args.es_url, args.target_index, docs)

    print_summary(docs, warnings)


if __name__ == "__main__":
    main()
