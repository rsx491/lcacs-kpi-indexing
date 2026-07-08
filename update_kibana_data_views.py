#!/usr/bin/env python3

import argparse
import json
import requests


SCRIPT_NAME = "update_kibana_data_views"
SCRIPT_VERSION = "1.0.0"

DEFAULT_KIBANA_URL = "http://localhost:5601"
DEFAULT_INDEX_PREFIX = "lcacs-kpi"
DEFAULT_INDEX_VERSION = "v1"


DATA_VIEWS = [
    {
        "label": "API Calls",
        "id": "34e87d03-1990-4a9e-a58b-d9f67b1cb61d",
        "suffix": "api-calls",
    },
    {
        "label": "Public Repo Downloads",
        "id": "924bd3ab-0e7f-4b33-a9df-7e7d63115de8",
        "suffix": "public-repo-downloads",
    },
    {
        "label": "Estimated Process Downloads",
        "id": "009f6f4b-bff4-4bc7-bc40-fa661b39f566",
        "suffix": "estimated-process-downloads",
    },
    {
        "label": "Public Process Inventory",
        "id": "c3985c2d-5281-4764-8ed3-8e19408e61f9",
        "suffix": "public-process-inventory",
    },
    {
        "label": "Total Repositories Published",
        "id": "bfda769b-add0-45fe-960d-fac01ce6d2c5",
        "suffix": "total-repositories-published",
    },
    {
        "label": "Release Activity Events",
        "id": "9aba98fa-5d13-4cfb-aa21-1b060b2ff39d",
        "suffix": "release-activity-events",
    },
]


def build_index_name(prefix: str, suffix: str, run_label: str, version: str) -> str:
    return f"{prefix}-{suffix}-{run_label}-{version}"


def update_data_view(kibana_url: str, data_view_id: str, index_name: str):
    url = f"{kibana_url.rstrip('/')}/api/data_views/data_view/{data_view_id}"

    payload = {
        "data_view": {
            "title": index_name,
            "name": index_name,
            "timeFieldName": "@timestamp",
        }
    }

    resp = requests.post(
        url,
        headers={
            "kbn-xsrf": "true",
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=60,
    )

    if not resp.ok:
        raise RuntimeError(
            f"Failed updating data view {data_view_id}: {resp.status_code} {resp.text}"
        )

    return resp.json()


def main():
    parser = argparse.ArgumentParser(
        description="Update Kibana data views to point at LCACS KPI reporting-period indexes."
    )

    parser.add_argument("--kibana-url", default=DEFAULT_KIBANA_URL)
    parser.add_argument("--index-prefix", default=DEFAULT_INDEX_PREFIX)
    parser.add_argument("--index-version", default=DEFAULT_INDEX_VERSION)
    parser.add_argument("--run-label", required=True)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    print(f"=== {SCRIPT_NAME} {SCRIPT_VERSION} ===")
    print(f"Kibana URL: {args.kibana_url}")
    print(f"Run label:  {args.run_label}")
    print(f"Dry run:    {args.dry_run}")

    for dv in DATA_VIEWS:
        index_name = build_index_name(
            args.index_prefix,
            dv["suffix"],
            args.run_label,
            args.index_version,
        )

        print(f"\n{dv['label']}")
        print(f"  Data view ID: {dv['id']}")
        print(f"  Target index: {index_name}")

        if args.dry_run:
            print("  DRY RUN: not updated")
            continue

        update_data_view(args.kibana_url, dv["id"], index_name)
        print("  Updated")

    print("\n=== Kibana data view update complete ===")


if __name__ == "__main__":
    main()