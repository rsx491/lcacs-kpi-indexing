# LCACS KPI QA

The QA layer validates KPI data before and after indexing so that only
validated KPI indexes are eligible for consolidation and export.

The QA workflow is part of the KPI indexing pipeline:

Raw weblogs
    -> Pre-processing QA
    -> KPI processing/indexing
    -> Post-processing QA
    -> Reconciliation
    -> PASS / FAIL
    -> Consolidation/export only on PASS

## Pre-processing QA

`pre_processing.py`

Validates the source data independently of the KPI processing implementation.

Responsibilities:

- Verify the source log exists and is readable.
- Validate the configured reporting window.
- Inspect timestamps against the configured start/end boundaries.
- Count raw log records.
- Count malformed/unparseable records.
- Count relevant endpoint populations needed for KPI sanity checks.
- Produce structured QA metrics for reconciliation.

Pre-processing QA should remain independent of the production KPI
transformation logic where practical. It should not reproduce the complete
KPI business algorithm.

## KPI Processing

The existing `index_*.py` scripts remain responsible for KPI business logic.

Examples include:

- parsing and classification
- qualification rules
- token correlation
- repository filtering
- deduplication
- document construction
- Elasticsearch indexing

Existing processor metrics provide observability into these transformations.

## Post-processing QA

`post_processing.py`

Validates what actually exists in Elasticsearch after indexing.

Responsibilities:

- Verify the expected index exists.
- Verify document counts.
- Validate required fields.
- Validate reporting-period boundaries.
- Detect unexpected null/missing values.
- Validate KPI-specific invariants.
- Detect indexing/rejection problems where available.
- Produce structured QA metrics for reconciliation.

## Reconciliation

`reconciliation.py`

Compares independent source QA, processor metrics, and Elasticsearch results.

Initial invariants include:

- Processor completed-document count must equal the Elasticsearch document count.
- Indexed documents must fall within the configured reporting window.
- Required KPI fields must be present.
- KPI-specific validation rules must pass.

A failed reconciliation blocks consolidation/export.

## Export Gate

An index existing in Elasticsearch does not mean that the index is valid.

Only indexes with successful QA and reconciliation are eligible for
consolidation/export.

    INDEXED != VALIDATED

    VALIDATED == ELIGIBLE FOR EXPORT

## Reporting Window

Reporting boundaries use:

    start <= timestamp < end

The start date is inclusive and the end date is exclusive.

The QA layer must validate the actual data against these boundaries rather
than assuming that a logfile name correctly describes its contents.
