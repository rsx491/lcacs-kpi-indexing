#!/usr/bin/env python3

import argparse
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

import requests


DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_DOWNLOAD_INDEX = "lcacs-kpi-public-repo-downloads-30d-v1"
DEFAULT_OLD_UNIT_INDEX = "lcacs-kpi-estimated-unit-process-downloads-poc"
DEFAULT_TARGET_INDEX = "lcacs-kpi-estimated-process-downloads-30d-v4"

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

                "unit_process_count_source": {"type": "keyword"},
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


def call_lcacs_process_search(group: str, repo_path: str) -> dict:
    params = {
        "group": group,
        "repositoryId": repo_path,
        "type": "PROCESS",
        "page": 1,
        "pageSize": 1
    }

    resp = requests.get(LCACS_SEARCH_URL, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


def extract_safe_api_counts(api_data: dict, repo_path: str) -> Tuple[Optional[int], Optional[int], str, str]:
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
            None,
            None,
            "api_not_repo_specific",
            "Requested repository was not present in repositoryId aggregation."
        )

    if len(repository_entries) > 1:
        return (
            None,
            None,
            "api_ambiguous",
            "API response contained multiple repositoryId buckets; processType counts may be group-level."
        )

    unit_count = 0
    lci_count = 0

    for entry in process_type_entries:
        key = entry.get("key")
        count = int(entry.get("count", 0) or 0)

        if key == "UNIT_PROCESS":
            unit_count = count

        if key == "LCI_RESULT":
            lci_count = count

    return (
        unit_count,
        lci_count,
        "api_repo_specific",
        "Repo-specific counts pulled from LCACS public search aggregation. UNIT_PROCESS=Unit Process; LCI_RESULT=System Process."
    )


def get_safe_api_counts_for_repo(repo_path: str) -> Dict[str, Any]:
    if "/" not in repo_path:
        return {
            "api_unit_process_count": None,
            "api_lci_result_count": None,
            "api_status": "bad_repo_path",
            "api_note": "Could not split repo_path into group/repo."
        }

    group, _repo = repo_path.split("/", 1)

    try:
        api_data = call_lcacs_process_search(group, repo_path)
        unit_count, lci_count, status, note = extract_safe_api_counts(api_data, repo_path)
        return {
            "api_unit_process_count": unit_count,
            "api_lci_result_count": lci_count,
            "api_status": status,
            "api_note": note
        }
    except Exception as e:
        return {
            "api_unit_process_count": None,
            "api_lci_result_count": None,
            "api_status": "api_error",
            "api_note": f"API lookup failed: {e}"
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
        old_unit_count = int(old.get("current_unit_process_count", 0)) if old else 0

        api_counts = get_safe_api_counts_for_repo(repo_path)

        api_unit = api_counts["api_unit_process_count"]
        api_lci = api_counts["api_lci_result_count"]
        api_status = api_counts["api_status"]
        api_note = api_counts["api_note"]

        # UNIT_PROCESS rule:
        # Use safe API unit count only if repo-specific.
        # Otherwise keep old validated unit count. Never zero known-good repo counts.
        if api_status == "api_repo_specific" and api_unit is not None:
            unit_count = int(api_unit)
            unit_source = "lcacs_public_search_api_repo_specific"
        else:
            unit_count = old_unit_count
            unit_source = old_unit_index if old else "missing_old_unit_count"

        # LCI_RESULT/System Process rule:
        # Use only safe repo-specific API LCI_RESULT count.
        # Otherwise default to 0, with note that it is unavailable/needs validation.
        if api_status == "api_repo_specific" and api_lci is not None:
            lci_count = int(api_lci)
            lci_source = "lcacs_public_search_api_repo_specific"
        else:
            lci_count = 0
            lci_source = "unavailable_due_to_ambiguous_or_missing_api_count"

        total_count = unit_count + lci_count

        if api_status == "api_repo_specific":
            process_status = "available"
            process_note = api_note
        elif unit_count > 0:
            process_status = "fallback_validated_unit_count_lci_unavailable"
            process_note = (
                f"{api_note} Used validated UNIT_PROCESS fallback from {old_unit_index}; "
                "LCI_RESULT/System Process count set to 0 because repo-specific LCI count was not safely available."
            )
        else:
            process_status = "needs_validation"
            process_note = (
                f"{api_note} No validated UNIT_PROCESS fallback was available; "
                "counts are incomplete and should be validated."
            )

        doc = {
            "@timestamp": now,

            "repo_path": repo_path,
            "group": group,
            "repo": repo,

            "completed_public_repo_downloads": int(downloads),

            "current_unit_process_count": int(unit_count),
            "current_lci_result_count": int(lci_count),
            "current_total_process_count": int(total_count),

            "estimated_unit_process_downloads": int(downloads) * int(unit_count),
            "estimated_lci_result_downloads": int(downloads) * int(lci_count),
            "estimated_total_process_downloads": int(downloads) * int(total_count),

            "unit_process_count_source": unit_source,
            "lci_result_count_source": lci_source,
            "process_count_status": process_status,
            "process_count_note": process_note,

            "download_count_source": download_index,
            "source": "lcacs_kpi_enrichment",
            "kpi_name": "estimated_process_downloads"
        }

        if process_status != "available":
            warnings.append(f"{repo_path}: {process_status} - {process_note}")

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
    fallback = sum(1 for d in docs if d["process_count_status"] == "fallback_validated_unit_count_lci_unavailable")
    needs_validation = sum(1 for d in docs if d["process_count_status"] == "needs_validation")

    print("\n=== ESTIMATED PROCESS DOWNLOADS V4 SUMMARY ===")
    print(f"Repositories indexed: {len(docs):,}")
    print(f"Repo-specific API process counts available: {available:,}")
    print(f"Fallback validated UNIT_PROCESS counts used: {fallback:,}")
    print(f"Repos still needing validation: {needs_validation:,}")

    print(f"Completed public repo downloads: {total_downloads:,}")

    print(f"Current UNIT_PROCESS count across downloaded repos: {total_unit:,}")
    print(f"Current LCI_RESULT/System Process count across downloaded repos: {total_lci:,}")
    print(f"Current total process count across downloaded repos: {total_process:,}")

    print(f"Estimated UNIT_PROCESS downloads: {est_unit:,}")
    print(f"Estimated LCI_RESULT/System Process downloads: {est_lci:,}")
    print(f"Estimated total process downloads: {est_total:,}")

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
        description="Create estimated process-download KPI index using public downloads, validated unit counts, and safe LCI_RESULT counts."
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
