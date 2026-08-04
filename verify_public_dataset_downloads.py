#!/usr/bin/env python3
"""
verify_public_dataset_downloads.py

Verifies completed public dataset download events directly from the raw
Apache web logs.

Definition:
    - HTTP 200
    - /lca-collaboration/ws/public/browse/{group}/{repo}/{type}/{refId}
    - {group}/{repo} exists in public_repo_keys.txt
"""

import re
from pathlib import Path

RAW_LOG_DIR = Path("lca_log_data/raw")
PUBLIC_REPO_FILE = Path("public_repo_keys.txt")

# ----------------------------------------------------------------------
# Load public repository whitelist
# ----------------------------------------------------------------------

PUBLIC_REPOS = {
    line.strip()
    for line in PUBLIC_REPO_FILE.read_text(encoding="utf-8").splitlines()
    if line.strip()
}

# ----------------------------------------------------------------------
# Apache access log parser
# ----------------------------------------------------------------------

LOG_RE = re.compile(
    r'^\S+\s+\S+\s+\S+\s+\S+\s+'
    r'\[[^\]]+\]\s+'
    r'"(?P<method>\S+)\s+(?P<request>\S+)\s+HTTP/[^"]+"\s+'
    r'(?P<status>\d{3})\s+'
)

# ----------------------------------------------------------------------
# Public dataset endpoint
# ----------------------------------------------------------------------

BROWSE_RE = re.compile(
    r'^/lca-collaboration/ws/public/browse/'
    r'(?P<group>[^/?#]+)/'
    r'(?P<repo>[^/?#]+)/'
    r'(?P<resource_type>[^/?#]+)/'
    r'(?P<resource_id>[^/?#]+)$'
)

# ----------------------------------------------------------------------
# Counters
# ----------------------------------------------------------------------

total_lines = 0
parsed_lines = 0
qualifying_events = 0

repository_counts = {}

# ----------------------------------------------------------------------
# Process logs
# ----------------------------------------------------------------------

for logfile in sorted(RAW_LOG_DIR.glob("*/*.log")):

    print(f"Reading {logfile}")

    with logfile.open(
        "r",
        encoding="utf-8",
        errors="replace"
    ) as infile:

        for line in infile:

            total_lines += 1

            match = LOG_RE.match(line)

            if not match:
                continue

            parsed_lines += 1

            if match.group("status") != "200":
                continue

            request = match.group("request")

            # Remove query string
            path = request.split("?", 1)[0]

            browse = BROWSE_RE.fullmatch(path)

            if browse is None:
                continue

            repository_key = (
                f"{browse.group('group')}/"
                f"{browse.group('repo')}"
            )

            if repository_key not in PUBLIC_REPOS:
                continue

            qualifying_events += 1

            repository_counts[repository_key] = (
                repository_counts.get(repository_key, 0) + 1
            )

# ----------------------------------------------------------------------
# Report
# ----------------------------------------------------------------------

print()
print("=" * 72)
print("Public Dataset Download Verification")
print("=" * 72)
print(f"Total raw log lines              : {total_lines:,}")
print(f"Successfully parsed log lines    : {parsed_lines:,}")
print(f"Qualifying dataset downloads     : {qualifying_events:,}")

print()
print("Top Public Repositories")
print("-" * 72)

for repository, count in sorted(
    repository_counts.items(),
    key=lambda x: x[1],
    reverse=True
):

    print(f"{count:>10,}  {repository}")
