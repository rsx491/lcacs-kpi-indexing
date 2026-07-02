#!/usr/bin/env python3

import argparse
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests


DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_DOWNLOAD_INDEX = "lcacs-kpi-public-repo-downloads-30d-v1"
DEFAULT_OLD_UNIT_INDEX = "lcacs-kpi-estimated-unit-process-downloads-poc"
DEFAULT_TARGET_INDEX = "lcacs-kpi-estimated-process-downloads-30d-v5"

LCACS_BROWSE_BASE = "https://www.lcacommons.gov/lca-collaboration/ws/public/browse"


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
                "commit_id": {"type": "keyword"},

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
                "kpi_name": {"type": "keyword"}
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
    payload = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"download_event_type": "download_prepare"}},
                    {"range": {"status": {"gte": 200, "lt": 300}}}
                ]
            }
        },
        "aggs": {
            "by_repo": {
                "terms": {
                    "field": "repo_path",
                    "size": 1000,
                    "order": {"_count": "desc"}
                }
            }
        }
    }

    data = es_request("GET", es_url, f"{download_index}/_search", payload)
    buckets = data.get("aggregations", {}).get("by_repo", {}).get("buckets", [])

    return {bucket["key"]: int(bucket["doc_count"]) for bucket in buckets}


def get_old_validated_unit_counts(es_url: str, old_index: str) -> Dict[str, Dict[str, Any]]:
    payload = {
        "size": 1000,
        "_source": [
            "repo_path",
            "repository_path",
            "agency_repo",
            "group",
            "repo",
            "current_unit_process_count",
            "unit_process_count"
        ],
        "query": {"match_all": {}}
    }

    data = es_request("GET", es_url, f"{old_index}/_search", payload)

    results = {}

    for hit in data.get("hits", {}).get("hits", []):
        src = hit.get("_source", {})

        repo_path = src.get("repo_path") or src.get("repository_path") or src.get("agency_repo")
        if not repo_path:
            continue

        group = src.get("group")
        repo = src.get("repo")

        if (not group or not repo) and "/" in repo_path:
            group, repo = repo_path.split("/", 1)

        unit_count = src.get("current_unit_process_count")
        if unit_count is None:
            unit_count = src.get("unit_process_count", 0)

        results[repo_path] = {
            "repo_path": repo_path,
            "group": group,
            "repo": repo,
            "current_unit_process_count": int(unit_count or 0)
        }

    return results


def get_total_process_count_from_browse(repo_path: str) -> Dict[str, Any]:
    if "/" not in repo_path:
        return {
            "current_total_process_count": 0,
            "commit_id": None,
            "status": "bad_repo_path",
            "note": "Could not split repo_path into group/repo."
        }

    group, repo = repo_path.split("/", 1)
    url = f"{LCACS_BROWSE_BASE}/{group}/{repo}"

    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        for entry in data.get("data", []):
            if entry.get("type") == "PROCESS" and entry.get("typeOfEntry") == "MODEL_TYPE":
                return {
                    "current_total_process_count": int(entry.get("count", 0) or 0),
                    "commit_id": entry.get("commitId"),
                    "status": "available",
                    "note": "Total PROCESS count pulled from repo-scoped public browse endpoint."
                }

        return {
            "current_total_process_count": 0,
            "commit_id": None,
            "status": "missing_process_entry",
            "note": "Browse endpoint response did not contain a PROCESS model-type entry."
        }

    except Exception as e:
        return {
            "current_total_process_count": 0,
            "commit_id": None,
            "status": "api_error",
            "note": f"Browse endpoint lookup failed: {e}"
        }


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
        timeout=120
    )

    if not resp.ok:
        raise RuntimeError(f"Bulk failed: {resp.status_code} {resp.text}")

    result = resp.json()
    if result.get("errors"):
        raise RuntimeError(f"Bulk had errors: {json.dumps(result)[:3000]}")


def build_docs(download_counts: Dict[str, int], old_unit_counts: Dict[str, Dict[str, Any]], download_index: str, old_unit_index: str):
    docs = []
    warnings = []
    now = datetime.now(timezone.utc).isoformat()

    for repo_path, downloads in sorted(download_counts.items(), key=lambda x: x[1], reverse=True):
        if "/" in repo_path:
            group, repo = repo_path.split("/", 1)
        else:
            group, repo = None, repo_path

        old = old_unit_counts.get(repo_path)
        unit_count = int(old.get("current_unit_process_count", 0)) if old else 0

        browse = get_total_process_count_from_browse(repo_path)
        total_count = int(browse["current_total_process_count"] or 0)

        if total_count > 0:
            if unit_count > 0:
                if total_count >= unit_count:
                    lci_count = max(total_count - unit_count, 0)
                    status = "available"
                    note = (
                        "Total PROCESS count pulled from repo-scoped browse endpoint. "
                        "UNIT_PROCESS count pulled from validated unit-count index. "
                        "LCI_RESULT/System Process count derived as PROCESS total minus UNIT_PROCESS."
                    )
                else:
                    # Important fallback:
                    # The browse total should never be lower than the validated unit-process count.
                    # If that happens, preserve the validated UNIT_PROCESS count and do not use
                    # the lower browse total.
                    total_count = unit_count
                    lci_count = 0
                    status = "fallback_validated_unit_count_browse_conflict"
                    note = (
                        f"Browse PROCESS total was lower than validated UNIT_PROCESS count. "
                        f"Using validated UNIT_PROCESS count as total. "
                        f"LCI_RESULT/System Process count set to 0 because the browse total "
                        f"could not safely be reconciled."
                    )
            else:
                # We have a repo-level PROCESS total, but no validated UNIT_PROCESS split.
                # The total is still useful for the main KPI.
                lci_count = 0
                status = "total_only_unit_missing"
                note = (
                    "Total PROCESS count pulled from repo-scoped browse endpoint, but no validated "
                    "UNIT_PROCESS count was available. Total process estimate is usable; "
                    "UNIT_PROCESS/LCI_RESULT split cannot be derived."
                )
        else:
            total_count = unit_count
            lci_count = 0
            status = "fallback_unit_only"
            note = (
                f"{browse['note']} Falling back to validated UNIT_PROCESS count only; "
                "LCI_RESULT/System Process count unavailable."
            )

        doc = {
            "@timestamp": now,

            "repo_path": repo_path,
            "group": group,
            "repo": repo,
            "commit_id": browse.get("commit_id"),

            "completed_public_repo_downloads": int(downloads),

            "current_unit_process_count": int(unit_count),
            "current_lci_result_count": int(lci_count),
            "current_total_process_count": int(total_count),

            "estimated_unit_process_downloads": int(downloads) * int(unit_count),
            "estimated_lci_result_downloads": int(downloads) * int(lci_count),
            "estimated_total_process_downloads": int(downloads) * int(total_count),

            "unit_process_count_source": old_unit_index if old else "missing_validated_unit_count",
            "total_process_count_source": "lcacs_public_browse_endpoint" if browse["status"] == "available" else "fallback_or_missing",
            "lci_result_count_source": "derived_total_minus_unit" if status == "available" else "unavailable_or_not_derived",

            "process_count_status": status,
            "process_count_note": note,

            "download_count_source": download_index,
            "source": "lcacs_kpi_enrichment",
            "kpi_name": "estimated_process_downloads"
        }

        if status != "available":
            warnings.append(f"{repo_path}: {status} - {note}")

        docs.append(doc)

    return docs, warnings


def print_summary(docs: list, warnings: list):
    total_downloads = sum(d["completed_public_repo_downloads"] for d in docs)

    total_unit = sum(d["current_unit_process_count"] for d in docs)
    total_lci = sum(d["current_lci_result_count"] for d in docs)
    total_process = sum(d["current_total_process_count"] for d in docs)

    est_unit = sum(d["estimated_unit_process_downloads"] for d in docs)
    est_lci = sum(d["estimated_lci_result_downloads"] for d in docs)
    est_total = sum(d["estimated_total_process_downloads"] for d in docs)

    available = sum(1 for d in docs if d["process_count_status"] == "available")
    not_available = len(docs) - available

    print("\n=== ESTIMATED PROCESS DOWNLOADS V5 SUMMARY ===")
    print(f"Repositories indexed: {len(docs):,}")
    print(f"Repositories with full derived process counts: {available:,}")
    print(f"Repositories needing review/fallback: {not_available:,}")

    print(f"Completed public repo downloads: {total_downloads:,}")

    print(f"Current UNIT_PROCESS count across downloaded repos: {total_unit:,}")
    print(f"Current LCI_RESULT/System Process count across downloaded repos: {total_lci:,}")
    print(f"Current total PROCESS count across downloaded repos: {total_process:,}")

    print(f"Estimated UNIT_PROCESS downloads: {est_unit:,}")
    print(f"Estimated LCI_RESULT/System Process downloads: {est_lci:,}")
    print(f"Estimated total PROCESS downloads: {est_total:,}")

    print("\nTop 20 repos by estimated total PROCESS downloads:")
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
        description="Create estimated process-download KPI index using public downloads and repo-scoped browse PROCESS totals."
    )

    parser.add_argument("--es-url", default=DEFAULT_ES_URL)
    parser.add_argument("--download-index", default=DEFAULT_DOWNLOAD_INDEX)
    parser.add_argument("--old-unit-index", default=DEFAULT_OLD_UNIT_INDEX)
    parser.add_argument("--target-index", default=DEFAULT_TARGET_INDEX)
    parser.add_argument("--recreate", action="store_true")

    args = parser.parse_args()

    create_target_index(args.es_url, args.target_index, recreate=args.recreate)

    print(f"Reading download counts from: {args.download_index}")
    download_counts = get_public_repo_download_counts(args.es_url, args.download_index)
    print(f"Found public repo download counts for {len(download_counts):,} repos.")

    print(f"Reading validated UNIT_PROCESS counts from: {args.old_unit_index}")
    old_unit_counts = get_old_validated_unit_counts(args.es_url, args.old_unit_index)
    print(f"Found validated UNIT_PROCESS counts for {len(old_unit_counts):,} repos.")

    docs, warnings = build_docs(
        download_counts=download_counts,
        old_unit_counts=old_unit_counts,
        download_index=args.download_index,
        old_unit_index=args.old_unit_index
    )

    bulk_index(args.es_url, args.target_index, docs)
    print_summary(docs, warnings)


if __name__ == "__main__":
    main()
