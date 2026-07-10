# Event-level estimated process-download KPI (v2)

## What changed

`index_estimated_process_downloads.py` now writes one derived document for each
successful public-repository `download_prepare` event instead of one annual
summary document per repository.

Each derived document retains the source event's:

- `@timestamp`
- `client_ip`
- request and endpoint
- HTTP method and status
- bytes and request time
- referrer and user agent
- repository/group fields

It then adds process-count and estimated-download fields. This allows Kibana to
filter any arbitrary start/end date without losing event detail.

`run_kpi_framework.py` now explicitly passes the annual run's generated public
repository-download index and public-process-inventory index into the estimated
process-download step. It no longer relies on the old 30-day default source
index names.

## Build a new version locally

Do not overwrite the validated v1 index on the first run. Use a new version:

```bash
python3 run_kpi_framework.py --log-file access_2024-09-30_to_2025-10-01.log.gz --es-url http://localhost:9200 --index-version v2 --recreate
```

The rebuilt estimated index will be:

```text
lcacs-kpi-estimated-process-downloads-2024-09-30-to-2025-10-01-v2
```

## Validation

Confirm timestamp coverage and document count:

```http
GET lcacs-kpi-estimated-process-downloads-2024-09-30-to-2025-10-01-v2/_search
{
  "size": 0,
  "aggs": {
    "earliest": { "min": { "field": "@timestamp" } },
    "latest": { "max": { "field": "@timestamp" } },
    "unique_source_events": { "cardinality": { "field": "source_event_id" } }
  }
}
```

Compare the full-period v1 total with the v2 sum:

```http
GET lcacs-kpi-estimated-process-downloads-2024-09-30-to-2025-10-01-v2/_search
{
  "size": 0,
  "aggs": {
    "downloads": { "sum": { "field": "completed_public_repo_downloads" } },
    "estimated_total": { "sum": { "field": "estimated_total_process_downloads" } }
  }
}
```

## Kibana change required

The old 28-row summary dashboard used `max()` per repository. The v2 event-level
index must use `sum()` for these fields:

- `completed_public_repo_downloads`
- `estimated_unit_process_downloads`
- `estimated_lci_result_downloads`
- `estimated_total_process_downloads`

Process inventory fields such as `current_unit_process_count` and
`current_total_process_count` may remain `max()` when shown as point-in-time
repository attributes.

Use `@timestamp` as the data-view time field.
