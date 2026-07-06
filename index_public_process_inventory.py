#!/usr/bin/env python3

import argparse
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests

SCRIPT_NAME = "index_public_process_inventory"
SCRIPT_VERSION = "1.0.0"

DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_OLD_UNIT_INDEX = "lcacs-kpi-estimated-unit-process-downloads-poc"
DEFAULT_TARGET_INDEX = "lcacs-kpi-public-process-inventory-v1"

LCACS_REPOSITORY_URL = "https://www.lcacommons.gov/lca-collaboration/ws/public/repository"
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
                "label": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "version": {"type": "keyword"},
                "release_date": {"type": "date"},
                "commit_id": {"type": "keyword"},
                "has_releases": {"type": "boolean"},

                "current_unit_process_count": {"type": "long"},
                "current_lci_result_count": {"type": "long"},
                "current_total_process_count": {"type": "long"},

                "unit_process_count_source": {"type": "keyword"},
                "total_process_count_source": {"type": "keyword"},
                "lci_result_count_source": {"type": "keyword"},

                "process_count_status": {"type": "keyword"},
                "process_count_note": {"type": "text"},

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

    es_request("PUT", es_url, index, mapping)
    print(f"Created index: {index}")


def get_public_repositories() -> list:
    resp = requests.get(LCACS_REPOSITORY_URL, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    repos = []

    for item in data:
        settings = item.get("settings", {}) or {}

        repo_path = (
            settings.get("repositoryPath")
            or item.get("repositoryPath")
            or item.get("path")
        )

        if not repo_path:
            group = item.get("group")
            repo = item.get("name")
            if group and repo:
                repo_path = f"{group}/{repo}"

        if not repo_path or "/" not in repo_path:
            continue

        group, repo = repo_path.split("/", 1)

        release_date_raw = settings.get("releaseDate")
        release_date = None
        if release_date_raw:
            try:
                # LCACS returns ms epoch.
                release_date = datetime.fromtimestamp(
                    int(release_date_raw) / 1000,
                    tz=timezone.utc
                ).isoformat()
            except Exception:
                release_date = None

        repos.append({
            "repo_path": repo_path,
            "group": group,
            "repo": repo,
            "label": settings.get("label") or item.get("label"),
            "version": settings.get("version"),
            "release_date": release_date,
            "has_releases": bool(item.get("hasReleases", False)),
        })

    return repos


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


def build_docs(
    public_repos: list,
    old_unit_counts: Dict[str, Dict[str, Any]],
    old_unit_index: str,
    start_date: str = None,
    end_date: str = None,
    run_label: str = "manual",
):    
    now = datetime.now(timezone.utc).isoformat()
    docs = []
    warnings = []

    for repo_info in sorted(public_repos, key=lambda x: x["repo_path"]):
        repo_path = repo_info["repo_path"]

        old = old_unit_counts.get(repo_path)
        unit_count = int(old.get("current_unit_process_count", 0)) if old else 0

        browse = get_total_process_count_from_browse(repo_path)
        browse_total = int(browse["current_total_process_count"] or 0)

        if browse_total > 0:
            if unit_count > 0:
                if browse_total >= unit_count:
                    total_count = browse_total
                    lci_count = total_count - unit_count
                    status = "available"
                    note = (
                        "Total PROCESS count pulled from repo-scoped browse endpoint. "
                        "UNIT_PROCESS count pulled from validated unit-count index. "
                        "LCI_RESULT/System Process count derived as PROCESS total minus UNIT_PROCESS."
                    )
                else:
                    total_count = unit_count
                    lci_count = 0
                    status = "fallback_validated_unit_count_browse_conflict"
                    note = (
                        f"Browse PROCESS total ({browse_total}) was lower than validated "
                        f"UNIT_PROCESS count ({unit_count}). Using validated UNIT_PROCESS count "
                        "as total. LCI_RESULT/System Process count set to 0."
                    )
            else:
                # We have a repo-level PROCESS total, but no validated UNIT_PROCESS split.
                # For the Unit Process KPI, treat the public PROCESS total as unit processes
                # unless/until an LCI_RESULT/System split is available.
                total_count = browse_total
                unit_count = browse_total
                lci_count = 0
                status = "inferred_unit_process_count_from_public_process_total"
                note = (
                    "Total PROCESS count pulled from repo-scoped browse endpoint, but no validated "
                    "UNIT_PROCESS/LCI_RESULT split was available. For the unit-process KPI, the public "
                    "PROCESS total is counted as UNIT_PROCESS unless a System Process split is later validated."
                )
        else:
            total_count = unit_count
            lci_count = 0
            status = "fallback_unit_only" if unit_count > 0 else "no_process_count_available"
            note = (
                f"{browse['note']} Falling back to validated UNIT_PROCESS count only; "
                "LCI_RESULT/System Process count unavailable."
            )

        doc = {
            "@timestamp": now,

            "repo_path": repo_path,
            "group": repo_info["group"],
            "repo": repo_info["repo"],
            "label": repo_info.get("label"),
            "version": repo_info.get("version"),
            "release_date": repo_info.get("release_date"),
            "commit_id": browse.get("commit_id"),
            "has_releases": repo_info.get("has_releases", False),

            "current_unit_process_count": int(unit_count),
            "current_lci_result_count": int(lci_count),
            "current_total_process_count": int(total_count),

            "unit_process_count_source": old_unit_index if old else "missing_validated_unit_count",
            "total_process_count_source": (
                "lcacs_public_browse_endpoint"
                if browse["status"] == "available"
                else "fallback_or_missing"
            ),
            "lci_result_count_source": (
                "derived_total_minus_unit"
                if status == "available"
                else "unavailable_or_not_derived"
            ),

            "process_count_status": status,
            "process_count_note": note,

            "source": "lcacs_public_repository_inventory",
            "kpi_name": "public_process_inventory",
            "kpi_definition": (
                "Current public process inventory by repository. Total PROCESS count is pulled from "
                "the LCACS public browse endpoint. UNIT_PROCESS count uses validated historical counts "
                "where available. LCI_RESULT/System Process is derived as total PROCESS minus UNIT_PROCESS."
            ),

            # Framework metadata
            "script_name": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "run_label": run_label,
            "kpi_period_start": start_date,
            "kpi_period_end": end_date,
            "generated_at": now,
        }

        if status != "available":
            warnings.append(f"{repo_path}: {status} - {note}")

        docs.append(doc)

    return docs, warnings


def print_summary(
    docs: list,
    warnings: list,
    start_date: str = None,
    end_date: str = None,
    run_label: str = "manual",
    dry_run: bool = False,
):
    repo_count = len(docs)
    unit_total = sum(d["current_unit_process_count"] for d in docs)
    lci_total = sum(d["current_lci_result_count"] for d in docs)
    process_total = sum(d["current_total_process_count"] for d in docs)

    available = sum(1 for d in docs if d["process_count_status"] == "available")
    needs_review = repo_count - available

    print("\n=== PUBLIC PROCESS INVENTORY SUMMARY ===")
    print(f"Script: {SCRIPT_NAME} {SCRIPT_VERSION}")
    print(f"Run label: {run_label}")
    print(f"KPI period: {start_date} to {end_date}")
    print(f"Dry run: {dry_run}")

    print(f"Public repositories indexed: {repo_count:,}")
    print(f"Repositories with derived Unit/System split: {available:,}")
    print(f"Repositories needing review or fallback: {needs_review:,}")

    print(f"Total UNIT_PROCESS published: {unit_total:,}")
    print(f"Total LCI_RESULT/System Process published: {lci_total:,}")
    print(f"Total PROCESS published: {process_total:,}")

    print("\nTop 20 repositories by total PROCESS count:")
    for d in sorted(docs, key=lambda x: x["current_total_process_count"], reverse=True)[:20]:
        print(
            f"  {d['current_total_process_count']:,}\t"
            f"{d['repo_path']}\t"
            f"unit={d['current_unit_process_count']:,}\t"
            f"system={d['current_lci_result_count']:,}\t"
            f"status={d['process_count_status']}"
        )

    if warnings:
        print("\nWARNINGS:")
        for warning in warnings:
            print(f"  {warning}")


def main():
    parser = argparse.ArgumentParser(
        description="Index current public process inventory by LCACS public repository."
    )
    parser.add_argument("--es-url", default=DEFAULT_ES_URL)
    parser.add_argument("--old-unit-index", default=DEFAULT_OLD_UNIT_INDEX)
    parser.add_argument("--index", default=DEFAULT_TARGET_INDEX)
    parser.add_argument("--start-date", help="Inclusive start date, YYYY-MM-DD")
    parser.add_argument("--end-date", help="Exclusive end date, YYYY-MM-DD")
    parser.add_argument("--run-label", default="manual")
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    create_target_index(args.es_url, args.index, recreate=args.recreate)

    print("Reading public repositories from LCACS public repository endpoint...")
    public_repos = get_public_repositories()
    print(f"Found public repositories: {len(public_repos):,}")

    print(f"Reading validated UNIT_PROCESS counts from: {args.old_unit_index}")
    old_unit_counts = get_old_validated_unit_counts(args.es_url, args.old_unit_index)
    print(f"Found validated UNIT_PROCESS counts for {len(old_unit_counts):,} repos.")

    docs, warnings = build_docs(
        public_repos,
        old_unit_counts,
        args.old_unit_index,
        start_date=args.start_date,
        end_date=args.end_date,
        run_label=args.run_label,
    )

    if not args.dry_run:
        bulk_index(args.es_url, args.index, docs)
    else:
        print("Dry run enabled; skipping bulk index.")

    print_summary(
        docs,
        warnings,
        start_date=args.start_date,
        end_date=args.end_date,
        run_label=args.run_label,
        dry_run=args.dry_run,
    )

if __name__ == "__main__":
    main()
