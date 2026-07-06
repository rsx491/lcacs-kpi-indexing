#!/usr/bin/env python3

import argparse
import re
import subprocess
import sys
from pathlib import Path


DATE_RANGE_RE = re.compile(
    r"access_(?P<start>\d{4}-\d{2}-\d{2})_to_(?P<end>\d{4}-\d{2}-\d{2})"
)


KPI_SCRIPTS = [
    {
        "name": "api_calls",
        "script": "index_api_calls.py",
        "index_suffix": "api-calls",
        "requires_log_file": True,
    },
    {
        "name": "public_repo_downloads",
        "script": "index_public_repo_downloads.py",
        "index_suffix": "public-repo-downloads",
        "requires_log_file": True,
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
        "requires_log_file": False,
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
        "requires_log_file": False,
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
    print(f"Start date: {start_date}")
    print(f"End date:   {end_date}")
    print(f"Run label:  {run_label}")
    print(f"ES URL:     {args.es_url}")
    print(f"Dry run:    {args.dry_run}")

    for kpi in KPI_SCRIPTS:
        script_path = repo_root / kpi["script"]

        if not script_path.exists():
            raise SystemExit(f"Missing KPI script: {script_path}")

        index_name = build_index_name(
            args.index_prefix,
            kpi["index_suffix"],
            run_label,
            args.index_version,
        )

        cmd = [
            sys.executable,
            str(script_path),
        ]

        if kpi["requires_log_file"]:
            if not log_file:
                raise SystemExit(f"{kpi['name']} requires --log-file")
            cmd.append(str(log_file))

        cmd.extend([
            "--es-url", args.es_url,
            "--index", index_name,
            "--start-date", start_date,
            "--end-date", end_date,
            "--run-label", run_label,
        ])

        if args.recreate:
            cmd.append("--recreate")

        if args.dry_run:
            cmd.append("--dry-run")

        run_command(cmd, dry_run=args.dry_run)

    print("\n=== KPI framework run complete ===")


if __name__ == "__main__":
    main()