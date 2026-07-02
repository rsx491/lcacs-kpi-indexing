#!/usr/bin/env python3

import argparse
import gzip
import re
from collections import Counter
from pathlib import Path

DATE_RE = re.compile(r"\[(\d{2})/(\w{3})/(\d{4}):")
REQ_RE = re.compile(r'"([A-Z]+)\s+([^"]+)\s+HTTP/[0-9.]+"\s+(\d{3})')

MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

def open_log(path):
    path = Path(path)
    if path.suffix == ".gz":
        return gzip.open(path, "rt", errors="replace")
    return path.open("rt", errors="replace")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_file", help="logs_30d_all.log or logs_30d_all.log.gz")
    args = parser.parse_args()

    total_lines = 0
    days = Counter()
    statuses = Counter()

    public_search_process = 0
    public_search_repo = 0
    public_download_prepare = 0
    public_ws_calls = 0
    api_gateway_calls = 0

    deployment_hits = 0
    automated_deployment_hits = 0

    repo_downloads = Counter()
    process_type_mentions = Counter()

    with open_log(args.log_file) as f:
        for line in f:
            total_lines += 1

            dm = DATE_RE.search(line)
            if dm:
                dd, mon, yyyy = dm.groups()
                days[f"{yyyy}-{MONTHS[mon]}-{dd}"] += 1

            rm = REQ_RE.search(line)
            if not rm:
                continue

            method, request, status = rm.groups()
            status = int(status)
            statuses[status] += 1

            if "/lca-collaboration/ws/" in request:
                public_ws_calls += 1

            if "/lca-collaboration/ws/public/download/json/prepare/" in request and status == 200:
                public_download_prepare += 1
                repo_path = request.split("/lca-collaboration/ws/public/download/json/prepare/", 1)[1]
                repo_path = repo_path.split("?", 1)[0].strip("/")
                parts = repo_path.split("/")
                if len(parts) >= 2:
                    repo_downloads[f"{parts[0]}/{parts[1]}"] += 1

            if "/lca-collaboration/ws/public/search" in request and "type=PROCESS" in request:
                public_search_process += 1

            if "/lca-collaboration/ws/public/search" in request and ("type=REPOSITORY" in request or "repository" in request.lower()):
                public_search_repo += 1

            if "processType=UNIT_PROCESS" in request:
                process_type_mentions["UNIT_PROCESS"] += 1

            if "processType=LCI_RESULT" in request:
                process_type_mentions["LCI_RESULT"] += 1

            if "FederalLCACommonsapi" in request or "api.nal.usda.gov" in request:
                api_gateway_calls += 1

            if "deploy" in line.lower() or "deployment" in line.lower():
                deployment_hits += 1

            if "github" in line.lower() or "actions" in line.lower() or "azure" in line.lower() or "automated" in line.lower():
                if "deploy" in line.lower() or "deployment" in line.lower():
                    automated_deployment_hits += 1

    success = sum(c for s, c in statuses.items() if s < 500)
    failed = sum(c for s, c in statuses.items() if s >= 500)
    uptime_ratio = success / (success + failed) if (success + failed) else 0

    print("\n=== RAW LOG VALIDATION ===")
    print(f"Total lines: {total_lines:,}")
    print(f"Unique days: {len(days)}")
    print(f"Date range: {min(days) if days else 'N/A'} to {max(days) if days else 'N/A'}")

    print("\n=== KPI DATA AVAILABILITY CHECK ===")

    print("\n1. Number of total repositories published")
    print(f"Evidence in logs: public repository/search references = {public_search_repo:,}")
    print("Status:", "FOUND" if public_search_repo else "NOT FOUND / likely needs LCACS API or repository metadata source")

    print("\n2. Number of total unit/processes published")
    print(f"Evidence in logs: public PROCESS search references = {public_search_process:,}")
    print(f"Process type URL mentions: {dict(process_type_mentions)}")
    print("Status:", "FOUND" if public_search_process else "NOT FOUND / likely needs LCACS API process metadata source")

    print("\n3. API availability / uptime")
    print(f"HTTP <500 count: {success:,}")
    print(f"HTTP >=500 count: {failed:,}")
    print(f"Availability ratio from logs: {uptime_ratio:.6f}")
    print("Status:", "FOUND" if statuses else "NOT FOUND")

    print("\n4. Number of deployments per time")
    print(f"Deployment-like log hits: {deployment_hits:,}")
    print("Status:", "FOUND" if deployment_hits else "NOT FOUND / likely needs deployment logs, CI/CD, or release source")

    print("\n5. Number of automated deployments per time")
    print(f"Automated deployment-like hits: {automated_deployment_hits:,}")
    print("Status:", "FOUND" if automated_deployment_hits else "NOT FOUND / likely needs GitHub Actions/Azure DevOps/deploy source")

    print("\n=== DOWNLOAD KPI SUPPORTING DATA ===")
    print(f"Completed public repository download prepare events: {public_download_prepare:,}")
    print(f"Unique downloaded repositories: {len(repo_downloads)}")
    print("Top 10 downloaded repositories:")
    for repo, count in repo_downloads.most_common(10):
        print(f"  {count:,}\t{repo}")

    print("\n=== API KPI CHECK ===")
    print(f"Generic /lca-collaboration/ws/ calls: {public_ws_calls:,}")
    print(f"api.nal.usda.gov / FederalLCACommonsapi calls: {api_gateway_calls:,}")
    print("Status:", "FOUND" if api_gateway_calls else "NOT FOUND in this log file / may require API gateway dump")

if __name__ == "__main__":
    main()
