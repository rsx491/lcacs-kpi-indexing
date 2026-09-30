#!/usr/bin/env python3

import argparse
import re
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from elasticsearch import Elasticsearch
from qa.post_processing import validate_index


DATE_RANGE_RE = re.compile(
    r"access_(?P<start>\d{4}-\d{2}-\d{2})_to_(?P<end>\d{4}-\d{2}-\d{2})"
)


KPI_SCRIPTS = [
    {
        "name": "api_calls",
        "script": "index_api_calls.py",
        "index_suffix": "api-calls",
        "requires_log_file": True,
        "qa": {
            "post_processing": {
                "timestamp_field": "@timestamp",
                "required_fields": (
                    "@timestamp",
                    "method",
                    "endpoint",
                    "api_endpoint_group",
                    "status",
                    "is_public_api_call",
                    "has_api_key",
                    "api_auth_type",
                ),
            },
        },
    },
    {
        "name": "completed_downloads",
        "script": "index_completed_downloads.py",
        "index_suffix": "public-repository-download-events",
        "requires_log_file": True,
        "qa": {
            "post_processing": {
                "timestamp_field": "@timestamp",
                "required_fields": (
                    "@timestamp",
                    "event_type",
                    "download_type",
                    "repository",
                    "resource_type",
                    "resource_id",
                    "client_identifier_type",
                    "client_identifier",
                    "retrieval_path",
                    "retrieval_status",
                    "business_definition_version",
                    "dedupe_rule",
                ),
            },
        },
    },
    {
        "name": "public_process_inventory",
        "script": "index_public_process_inventory.py",
        "index_suffix": "public-process-inventory",
        "requires_log_file": False,
    },
    {
        "name": "total_repositories_published",
        "script": "index_total_repositories_published.py",
        "index_suffix": "total-repositories-published",
        "requires_log_file": True,
    },
    {
        "name": "estimated_process_downloads",
        "script": "index_estimated_process_downloads.py",
        "index_suffix": "estimated-process-downloads",
        "requires_log_file": False,
    },
    {
        "name": "release_activity",
        "script": "index_release_activity.py",
        "index_suffix": "release-activity",
        "requires_log_file": True,
        "qa": {
            "post_processing": {
                "output": "events",
                "timestamp_field": "@timestamp",
                "required_fields": (
                    "@timestamp",
                    "activity_type",
                    "method",
                    "status",
                    "request_path",
                ),
            },
        },
    },
]


def infer_dates_from_log_filename(path: Path):
    match = DATE_RANGE_RE.search(path.name)
    if not match:
        return None, None

    return match.group("start"), match.group("end")


def run_command(cmd: list[str], dry_run: bool = False):
    print("\n$ " + " ".join(cmd))

    if dry_run:
        print("DRY RUN: command not executed")
        return

    result = subprocess.run(cmd)

    if result.returncode != 0:
        raise SystemExit(result.returncode)


def build_index_name(prefix: str, suffix: str, run_label: str, version: str) -> str:
    return f"{prefix}-{suffix}-{run_label}-{version}"

def build_consolidated_index(
    es_url: str,
    target_index: str,
    source_indexes: list[str],
    start_date: str,
    end_date: str,
    recreate: bool = False,
    dry_run: bool = False,
    ):
    print("\n=== Consolidated KPI Index ===")
    print(f"Target index: {target_index}")

    if dry_run:
        for source_index in source_indexes:
            print(f"Would reindex: {source_index} -> {target_index}")
        return

    if recreate:
        response = requests.delete(
            f"{es_url}/{target_index}",
            timeout=60,
        )

        if response.status_code not in (200, 404):
            raise RuntimeError(
                f"Failed deleting consolidated index: "
                f"{response.status_code} {response.text}"
            )

    # Let Elasticsearch/OpenSearch create the target from the
    # first reindex operation for now. Each document receives
    # common framework metadata identifying its source.
    for source_index in source_indexes:
        exists = requests.head(
            f"{es_url}/{source_index}",
            timeout=30,
        )

        if exists.status_code != 200:
            raise RuntimeError(
                f"Required source index does not exist: {source_index}"
            )

        payload = {
            "source": {
                "index": source_index,
            },
            "dest": {
                "index": target_index,
            },
            "script": {
                "lang": "painless",
                "source": """
                    ctx._source['source_index'] = params.source_index;
                    ctx._source['period_start'] = params.start_date;
                    ctx._source['period_end'] = params.end_date;
                """,
                "params": {
                    "source_index": source_index,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            },
        }

        print(f"Reindexing: {source_index} -> {target_index}")

        response = requests.post(
            f"{es_url}/_reindex?wait_for_completion=true",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=3600,
        )

        if not response.ok:
            raise RuntimeError(
                f"Reindex failed for {source_index}: "
                f"{response.status_code} {response.text}"
            )

        result = response.json()

        if result.get("failures"):
            raise RuntimeError(
                f"Reindex contained failures for {source_index}: "
                f"{json.dumps(result['failures'])[:5000]}"
            )

        print(
            f"Copied {result.get('created', 0):,} documents "
            f"from {source_index}"
        )

    print(f"Consolidated index complete: {target_index}")


def main():
    parser = argparse.ArgumentParser(
        description="Run LCACS KPI indexing scripts for a reporting period."
    )

    parser.add_argument("--start-date", help="Inclusive start date, YYYY-MM-DD")
    parser.add_argument("--end-date", help="Exclusive end date, YYYY-MM-DD")
    parser.add_argument("--log-file", help="Production-exported combined access log")
    parser.add_argument("--es-url", default="http://localhost:9200")
    parser.add_argument("--run-label", help="Example: annual-2025")
    parser.add_argument("--index-prefix", default="lcacs-kpi")
    parser.add_argument("--index-version", default="v1")

    parser.add_argument(
        "--old-unit-index",
        default="lcacs-kpi-estimated-unit-process-downloads-poc",
    )

    parser.add_argument(
        "--output-mode",
        choices=["individual", "consolidated", "both"],
        default="both",
        help="Choose individual KPI indexes, one consolidated index, or both.",
    )

    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent

    if args.log_file:
        log_file = Path(args.log_file).expanduser().resolve()
    else:
        log_file = None

    if log_file and not log_file.exists():
        raise SystemExit(f"Log file not found: {log_file}")

    inferred_start = None
    inferred_end = None

    if log_file:
        inferred_start, inferred_end = infer_dates_from_log_filename(log_file)

    start_date = args.start_date or inferred_start
    end_date = args.end_date or inferred_end

    if not start_date or not end_date:
        raise SystemExit(
            "Start/end dates are required. Provide --start-date and --end-date, "
            "or use a log filename like access_2024-09-30_to_2025-10-01.log.gz."
        )

    run_label = args.run_label or f"{start_date}-to-{end_date}"

    print("=== LCACS KPI Framework Run ===")
    print(f"Start date:  {start_date}")
    print(f"End date:    {end_date}")
    print(f"Run label:   {run_label}")
    print(f"ES URL:      {args.es_url}")
    print(f"Output mode: {args.output_mode}")
    print(f"Dry run:     {args.dry_run}")

    index_names = {
        kpi["name"]: build_index_name(
            args.index_prefix,
            kpi["index_suffix"],
            run_label,
            args.index_version,
        )
        for kpi in KPI_SCRIPTS
    }

    if args.output_mode in ("individual", "both"):
        for kpi in KPI_SCRIPTS:
            script_path = repo_root / kpi["script"]

            if not script_path.exists():
                raise SystemExit(f"Missing KPI script: {script_path}")

            index_name = index_names[kpi["name"]]

            cmd = [
                sys.executable,
                str(script_path),
            ]

            if kpi["requires_log_file"]:
                if not log_file:
                    raise SystemExit(
                        f"{kpi['name']} requires --log-file"
                    )
                cmd.append(str(log_file))

            cmd.extend([
                "--es-url", args.es_url,
                "--index", index_name,
                "--start-date", start_date,
                "--end-date", end_date,
                "--run-label", run_label,
            ])

            if kpi["name"] == "public_process_inventory":
                cmd.extend([
                    "--old-unit-index",
                    args.old_unit_index,
                ])

            if kpi["name"] == "estimated_process_downloads":
                cmd.extend([
                    "--download-index",
                    index_names["completed_downloads"],
                    "--inventory-index",
                    index_names["public_process_inventory"],
                ])

            if kpi["name"] == "release_activity":
                events_index_name = build_index_name(
                    args.index_prefix,
                    "release-activity-events",
                    run_label,
                    args.index_version,
                )

                cmd.extend([
                    "--events-index",
                    events_index_name,
                ])

            if args.recreate:
                cmd.append("--recreate")

            if args.dry_run:
                cmd.append("--dry-run")

            run_command(
                cmd,
                dry_run=args.dry_run,
            )

            # Post-processing QA
            qa_config = kpi.get("qa", {}).get("post_processing")

            if qa_config and not args.dry_run:
                qa_index_name = index_name

            if qa_config.get("output") == "events":
                qa_index_name = events_index_name

            qa_result = validate_index(
                es=Elasticsearch(args.es_url),
                index_name=qa_index_name,
                start_date=datetime.fromisoformat(start_date),
                end_date=datetime.fromisoformat(end_date),
                timestamp_field=qa_config["timestamp_field"],
                required_fields=qa_config.get("required_fields", ()),
            )

    print(f"\nQA result for {kpi['name']}:")
    print(qa_result)

    if not qa_result.passed:
        raise SystemExit(
            f"QA FAILED for {kpi['name']}: "
            + "; ".join(qa_result.errors)
        )

        if args.output_mode in ("consolidated", "both"):
            consolidated_index = (
                f"{args.index_prefix}-"
                f"{run_label}-"
                f"{args.index_version}"
            )

            source_indexes = list(index_names.values())

            release_events_index = build_index_name(
                args.index_prefix,
                "release-activity-events",
                run_label,
                args.index_version,
            )

            source_indexes.append(release_events_index)

            build_consolidated_index(
                es_url=args.es_url,
                target_index=consolidated_index,
                source_indexes=source_indexes,
                start_date=start_date,
                end_date=end_date,
                recreate=args.recreate,
                dry_run=args.dry_run,
            )

        print("\n=== KPI framework run complete ===")


if __name__ == "__main__":
    main()