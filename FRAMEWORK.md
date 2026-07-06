# LCACS KPI Indexing Framework Contract

## 1. Purpose

Generate LCACS KPI OpenSearch indexes for a requested reporting period using exported LCACS web server logs.

---

## 2. Execution Model

The framework begins after production log collection.

Typical workflow:

1. Run the production log export command for the requested reporting period.
2. Download the exported logs to the MacBook development environment.
3. Execute KPI indexing locally.
4. Validate KPI indexes locally.
5. Export validated OpenSearch indexes.
6. Import validated artifacts into Stage.

The framework does **not** collect production logs directly.

---

## 3. Standard CLI Arguments

Every KPI script should eventually support:

```text
--start-date YYYY-MM-DD
--end-date YYYY-MM-DD
--log-dir PATH
--log-file PATH
--es-url URL
--index INDEX_NAME
--run-label LABEL
--recreate
--dry-run
```

---

## 4. Required Document Metadata

Every indexed document should include:

```text
kpi_name
script_name
script_version
run_label
kpi_period_start
kpi_period_end
generated_at
source
```

---

## 5. Index Naming Convention

Examples:

```text
lcacs-kpi-api-calls-annual-2025-v1

lcacs-kpi-public-repo-downloads-2025-01-01-to-2025-12-31-v1
```

---

## 6. Reporting Period Semantics

The reporting period is defined by the exported production log set.

Framework parameters:

```text
--start-date
--end-date
```

represent the reporting period metadata and validation boundaries.

Recommended convention:

- `start-date` is **inclusive**
- `end-date` is **exclusive**

Example:

```text
--start-date 2025-01-01
--end-date   2026-01-01
```

The framework assumes the supplied logs correspond to the requested reporting period.

---

## 7. Input Expectations

Accepted inputs include:

- Combined plain-text log
- Combined compressed (`.gz`) log
- Directory containing `lca_access.log*`
- Downloaded `/app/weblogs` files
- Production-exported reporting-period log bundle

The production log export process is outside the scope of this framework.

---

## 8. Output Expectations

Each KPI produces:

- OpenSearch index
- Summary printed to stdout
- Optional validation JSON
- Optional export artifacts

---

## 9. Validation Requirements

Before export, validate:

- Index exists
- Document count > 0
- Required fields exist
- Reporting period metadata matches the requested period
- Dashboard-critical fields are populated

---

## 10. Dependency Order

Some KPIs depend on others.

Example:

```text
public_repo_downloads
        ↓
estimated_process_downloads
```

Dependencies should be documented by each KPI.

---

## 11. Failure Behavior

The framework should:

- Fail fast on missing required input
- Fail fast on OpenSearch errors
- Allow warnings for missing repository metadata
- Never silently skip parsing failures
- Clearly report document counts and skipped records

---

## 12. Security / Data Handling

Requirements:

- API keys must be redacted
- Raw logs are never committed
- Generated OpenSearch exports are never committed
- Validation artifacts are not committed unless intentionally versioned
- Secrets and credentials are never committed

---

## 13. Export / Import Model

Validation always occurs locally.

Only validated indexes are exported for Stage import.

The framework assumes production log export has already completed before KPI generation begins.

---

## 14. Versioning

Every canonical KPI script includes:

```text
SCRIPT_NAME
SCRIPT_VERSION
```

The framework itself may introduce its own version identifier in the future.

---

## 15. Backward Compatibility

Archived scripts are retained for historical reference only.

Canonical scripts represent the supported implementations that conform to this framework contract.