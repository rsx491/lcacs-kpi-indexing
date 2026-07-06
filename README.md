# LCACS KPI Indexing

Python indexing scripts for generating LCACS KPI OpenSearch indexes from LCA Commons web logs.

## Workflow

1. Download web logs from `/app/weblogs`.
2. Run KPI indexing locally on the MacBook development environment.
3. Validate indexes locally using the containerized OpenSearch/Kibana stack.
4. Export validated index artifacts.
5. Import validated artifacts into Stage.

## Canonical KPI Scripts

- `index_api_calls.py`
- `index_public_repo_downloads.py`
- `index_public_process_inventory.py`
- `index_estimated_process_downloads.py`
- `index_total_repositories_published.py`
- `index_release_activity.py`

## Archive

Historical script versions are preserved under `archive/`.

## Goal

Build a parameterized KPI indexing framework that can generate indexes for arbitrary date ranges, such as 30-day, annual, or custom reporting periods, from a single execution entry point.
