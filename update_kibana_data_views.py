#!/usr/bin/env python3

import argparse
import json
import requests


SCRIPT_NAME = "update_kibana_data_views"
SCRIPT_VERSION = "1.0.0"

DEFAULT_KIBANA_URL = "http://localhost:5601"
DEFAULT_INDEX_PREFIX = "lcacs-kpi"
DEFAULT_INDEX_VERSION = "v1"


# Multiple dashboards reference different historical data view IDs.
# The target index name is constructed from the run label, not hard-coded.
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
        "label": "Estimated Process Downloads v3",
        "id": "d46037c2-9957-4afb-9b38-da888f1d04b5",
        "suffix": "estimated-process-downloads",
    },
    {
        "label": "Estimated Process Downloads v6",
        "id": "79a23877-e516-4f5e-bb95-cda17a67fe14",
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

DASHBOARDS = [
    {
        "label": "Public Process Inventory",
        "id": "84015897-96dd-4b72-a511-9e7bc51ad263",
    },
    {
        "label": "Public Repo Downloads",
        "id": "1f0af5e9-d11d-41a1-9bec-db73fd687293",
    },
    {
        "label": "Estimated Process Downloads",
        "id": "9e13d5e2-2f2e-4f9a-80d9-d8742f2a1a19",
    },
    {
        "label": "API Calls",
        "id": "ea218c32-3107-49c3-9bce-4c4f93089d56",
    },
    {
        "label": "Release Activity",
        "id": "32d99fcc-376b-4510-8527-9fa6d152ec67",
    },
    {
        "label": "Total Repositories Published",
        "id": "9c19f676-567e-46f6-9b70-04e435a385e8",
    },
]

def parse_run_label_dates(run_label: str):
    parts = run_label.split("-to-")
    if len(parts) != 2:
        raise ValueError(
            f"Run label must look like YYYY-MM-DD-to-YYYY-MM-DD, got: {run_label}"
        )

    start_date, end_date = parts
    return (
        f"{start_date}T00:00:00.000Z",
        f"{end_date}T23:59:59.999Z",
    )


def update_dashboard_time(kibana_url: str, dashboard_id: str, time_from: str, time_to: str):
    url = f"{kibana_url.rstrip('/')}/api/saved_objects/dashboard/{dashboard_id}"

    payload = {
        "attributes": {
            "timeRestore": True,
            "timeFrom": time_from,
            "timeTo": time_to,
        }
    }

    resp = requests.put(
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
            f"Failed updating dashboard {dashboard_id}: {resp.status_code} {resp.text}"
        )

    return resp.json()


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
    parser.add_argument("--update-dashboard-time", action="store_true")

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


    if args.update_dashboard_time:
        time_from, time_to = parse_run_label_dates(args.run_label)

        print("\n=== Updating dashboard time ranges ===")
        print(f"Time from: {time_from}")
        print(f"Time to:   {time_to}")

        for dashboard in DASHBOARDS:
            print(f"\n{dashboard['label']}")
            print(f"  Dashboard ID: {dashboard['id']}")

            if args.dry_run:
                print("  DRY RUN: dashboard time not updated")
                continue

            update_dashboard_time(
                args.kibana_url,
                dashboard["id"],
                time_from,
                time_to,
            )
            print("  Updated")

    print("\n=== Kibana update complete ===")

if __name__ == "__main__":
    main()