# LCACS KPI Indexing Framework Contract

1. Purpose
What the framework does: generate LCACS KPI OpenSearch indexes for a requested date range.

2. Execution model
MacBook/dev first, local validation, then export/import to Stage.

3. Standard CLI arguments
Every KPI should eventually support:
  --start-date YYYY-MM-DD
  --end-date YYYY-MM-DD
  --log-dir PATH
  --log-file PATH
  --es-url URL
  --index INDEX_NAME
  --run-label LABEL
  --recreate
  --dry-run

4. Required document metadata
Every indexed document should include:
  kpi_name
  script_name
  script_version
  run_label
  kpi_period_start
  kpi_period_end
  generated_at
  source

5. Index naming convention
Example:
  lcacs-kpi-api-calls-annual-2025-v1
  lcacs-kpi-public-repo-downloads-2025-01-01-to-2025-12-31-v1

6. Date-range semantics
Define whether:
  start-date is inclusive
  end-date is exclusive or inclusive

I strongly recommend:
  start-date inclusive
  end-date exclusive

Example:
  --start-date 2025-01-01
  --end-date 2026-01-01

7. Input expectations
Define accepted inputs:
  combined plain log
  combined .gz log
  directory of lca_access.log*
  downloaded /app/weblogs files

8. Output expectations
Each KPI produces:
  OpenSearch index
  summary printed to stdout
  optional validation JSON
  optional export artifacts

9. Validation requirements
Before export, validate:
  index exists
  document count > 0
  required fields exist
  date range matches expected period
  dashboard-critical fields are populated

10. Dependency order
Some KPIs depend on others.

Example:
  public_repo_downloads must run before estimated_process_downloads

11. Failure behavior
Define:
  fail fast on missing required input
  fail fast on OpenSearch errors
  allow warnings for missing repo metadata
  do not silently skip large parsing failures

12. Security/data handling
Define:
  API keys must be redacted
  raw logs are not committed
  generated exports are not committed
  no secrets in Git

13. Export/import model
Dev validates locally.
Only validated artifacts are exported for Stage.

14. Versioning
Every script has:
  SCRIPT_NAME
  SCRIPT_VERSION

Framework version can be separate later.

15. Backward compatibility
Archived scripts are historical only.
Canonical scripts are the framework targets.
