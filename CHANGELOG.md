# Changelog

All notable changes to this project are recorded here. Format based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [0.1.0] - 2026-10-21

### Added
- Config-driven pipeline (`src/sbl/conf/*.yml`): sources, silver tables, gold docs, quality rules, run order;
  `sbl-validate-config`.
- Bronze: append-only ingestion of CSV and Open-Meteo JSON with lineage columns, quarantine table, schema-drift
  policy and file-level re-run safety.
- Open-Meteo batch extract (one call per site and month, skips existing).
- Silver: incremental MERGE on business keys with watermarks; Delta CHECK constraints.
- Gold: `dim_building`, `dim_date`, `fact_daily_weather`, `fact_daily_energy` with kWh/m², degree days,
  weather-normalised ratio and anomaly flags.
- Data-quality rules (`ops.dq_results`), reconciliation (`ops.reconciliation`), audit log (`ops.audit_runs`),
  Delta RESTORE demo, monitoring SQL and a Markdown run report.
- 56 tests: unit, integration and reconciliation.
- GitHub Actions CI and tag-triggered release; Azure DevOps pipeline and Databricks Asset Bundle (not yet run).
- Docs: README, runbook, data dictionary, Power BI guide, data sources, run results.

### Fixed during development
- SQL timestamp formats containing quotes (`yyyy-MM-dd'T'HH:mm`) broke the cast expression.
- The references rule counted NULL keys as orphans.
- NaN residual spreads flagged false anomalies (Spark orders NaN above every number).

## [0.0.1] - 2026-10-20

### Added
- M0: repository skeleton, environment config, Unity Catalog setup SQL, Docker test runner,
  Delta smoke test, data-source and free-tier documentation.
