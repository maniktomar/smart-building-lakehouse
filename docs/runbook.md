# Operational runbook

Commands are shown for local runs (`--env local`, inside the Docker test image) and for Databricks
(`--env databricks`, as job tasks or from a notebook with the wheel installed). Table names are
written `ops.audit_runs` etc.; on Databricks prefix the catalog (`smart_building.ops.audit_runs`).

## 1. How to monitor

| What | Where |
|---|---|
| Did the last run succeed? | `run_history` and `failed_steps` queries in `src/sbl/sql/monitoring.sql` |
| What did each step do? | `ops.audit_runs`: one row per step and table, with rows read / written / quarantined, status and message |
| Which files were loaded? | `ops.ingested_files`: file path, batch id, rows read |
| What was rejected and why? | `ops.quarantine`: source, file, reason, raw record |
| Data-quality results | `ops.dq_results`: rule, severity, PASS/FAIL, failed rows |
| Reconciliation | `ops.reconciliation`: each check, both sides, relative difference, tolerance |
| Incremental position | `ops.watermarks`: last bronze `_ingested_at` processed per silver table |

Get the monitoring SQL with real table names:

```bash
python -m sbl.report --env databricks --render-sql
```

On Databricks, paste those queries into a SQL dashboard, and create a SQL alert on `failed_steps`
("trigger when rows > 0"). The job itself emails the owner when any task fails (`databricks.yml`).
Locally, `python -m sbl.report --env local` writes all of it to `docs/run-results.md`.

## 2. Re-running a failed batch

Every step is safe to re-run:

- **bronze** skips files already in `ops.ingested_files`. A file is only recorded there *after* its
  rows are written, so a failed batch is picked up again on the next run.
- **silver** processes bronze rows newer than its watermark and upserts with MERGE, so re-running
  never duplicates rows.
- **gold** is rebuilt from silver each run.

So the fix for most failures is: fix the cause, then re-run the failed stage and the ones after it.

```bash
sbl-run --env databricks --stage bronze      # then silver, gold, quality, reconcile
sbl-run --env databricks --stage all         # or everything
```

On Databricks you can also use "Repair run" on the failed job run, which re-runs only the failed
task and its downstream tasks.

**Re-process silver from scratch** (e.g. after changing a cast in `silver.yml`):

```bash
sbl-run --env databricks --stage silver --reset-watermark
```

**Re-load a file that was already ingested** (e.g. the source re-delivered a corrected file under
the same name): delete its row from `ops.ingested_files`, then re-run bronze and silver. Bronze will
hold both copies (it is append-only); silver keeps the most recently ingested row per key.

```sql
DELETE FROM ops.ingested_files WHERE file_path LIKE '%electricity_2017-01.csv';
```

Known gap (simplification): if a run crashes *between* writing bronze rows and recording the file,
the file is ingested twice into bronze. Silver's MERGE removes the duplicate, and reconciliation
check "source rows = bronze + quarantine" will flag the bronze surplus so you notice.

## 3. Restoring a table

Delta keeps every version of a table. To undo a bad load:

```sql
DESCRIBE HISTORY silver.meter_readings;                        -- find the last good version
SELECT count(*), sum(reading_kwh)
  FROM silver.meter_readings VERSION AS OF 12;                 -- check it (time travel)
RESTORE TABLE silver.meter_readings TO VERSION AS OF 12;       -- put it back
```

Then rebuild downstream: `sbl-run --stage gold`, then `quality` and `reconcile`.

`python -m sbl.restore_demo --env local` runs this end to end (bad MERGE, range rule catches it,
RESTORE, totals checked); its result is recorded in `ops.audit_runs` with step `restore`.

Limits: time travel only reaches versions whose files have not been removed by `VACUUM`
(default retention 7 days). Restoring a silver table does not move its watermark; if you restore
to before a batch was merged and want that batch re-applied, use `--reset-watermark`.

## 4. Common errors and fixes

| Error (in `ops.audit_runs.message`) | Cause | Fix |
|---|---|---|
| `SchemaDriftError: configured columns missing from files: [...]` | A source file lacks a column the config relies on | Check the file. If the source really changed, update `sources.yml` and `silver.yml` |
| `SchemaDriftError: new columns not allowed by config` | New column and `schema_drift.new_columns: fail` | Decide if you want it: switch to `add`, or ask the source to remove it |
| Message contains `schema drift: added columns [...]` (status SUCCESS) | Additive drift was accepted | Nothing to fix; map the column in `silver.yml` if you need it downstream |
| `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES ... reading_not_negative` | A negative reading reached silver | Find it in bronze, fix at source or quarantine it, re-run silver |
| `DataQualityError: N error rule(s) failed: ...` | An `error` rule failed; results are in `ops.dq_results` | Query `dq_latest`, fix the data, re-run `quality` |
| `ReconciliationError: ...` | Counts or totals disagree between layers | Query `reconciliation_latest` to see which layer lost or gained rows |
| `ConfigError: invalid config` | YAML mistake | Run `sbl-validate-config` locally; CI runs it on every PR |
| `HTTPError` / `URLError` in extract | Open-Meteo unavailable or rate-limited (free tier: <10,000 calls/day) | Re-run `extract_weather`; already downloaded months are skipped |
| `[TABLE_OR_VIEW_NOT_FOUND]` on Databricks | Catalog or schema missing, or wrong `--catalog` | Run `setup/00_workspace_setup.sql`, or pass `--catalog workspace` |
| `HADOOP_HOME and hadoop.home.dir are unset` (local) | PySpark on native Windows | Use the Docker image (`Dockerfile.test`) |

## 5. Azure DevOps

`azure-pipelines.yml` mirrors the GitHub Actions workflows. It has **not been run**: a new Azure
DevOps organisation must request the free Microsoft-hosted parallel job before pipelines run. To try
it: create an organisation and project, request the free tier grant, then
Pipelines > New pipeline > GitHub > this repo > "Existing Azure Pipelines YAML file".
