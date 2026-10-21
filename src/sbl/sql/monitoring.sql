-- Monitoring queries over the audit, quality and reconciliation tables.
-- {layer.table} placeholders are filled in by `python -m sbl.report`:
--   python -m sbl.report --env databricks --render-sql   prints these with Unity Catalog names,
--   ready to paste into a Databricks SQL dashboard or alert.

-- name: run_history
SELECT run_id,
       min(started_at)                                     AS started_at,
       max(ended_at)                                       AS ended_at,
       count(*)                                            AS steps,
       sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)  AS failed_steps,
       sum(rows_written)                                   AS rows_written,
       sum(rows_quarantined)                               AS rows_quarantined
FROM {ops.audit_runs}
GROUP BY run_id
ORDER BY started_at;

-- name: audit_log
SELECT run_id, step, target_table, status, rows_read, rows_written, rows_quarantined,
       round(unix_timestamp(ended_at) - unix_timestamp(started_at), 0) AS seconds,
       message
FROM {ops.audit_runs}
ORDER BY started_at;

-- name: failed_steps
-- Use as a Databricks SQL alert: trigger when the query returns any rows.
SELECT run_id, step, target_table, ended_at, message
FROM {ops.audit_runs}
WHERE status = 'FAILED'
ORDER BY ended_at DESC;

-- name: quarantine_by_reason
SELECT source, reason, count(*) AS rows, count(DISTINCT _source_file) AS files
FROM {ops.quarantine}
GROUP BY source, reason
ORDER BY rows DESC;

-- name: dq_latest
SELECT table_name, rule_name, severity, status, failed_rows, total_rows, details
FROM {ops.dq_results}
WHERE run_id = (SELECT max_by(run_id, checked_at) FROM {ops.dq_results})
ORDER BY status, table_name, rule_name;

-- name: reconciliation_latest
SELECT check_name, left_value, right_value, relative_diff, tolerance, status
FROM {ops.reconciliation}
WHERE run_id = (SELECT max_by(run_id, checked_at) FROM {ops.reconciliation})
ORDER BY check_name;

-- name: energy_by_building
SELECT b.building_id, b.primary_use, b.floor_area_sqm,
       count(*)                                                        AS days,
       round(sum(f.energy_kwh), 0)                                     AS total_kwh,
       round(sum(f.energy_kwh) / b.floor_area_sqm, 1)                  AS kwh_per_sqm,
       sum(CASE WHEN f.data_quality_flag = 'incomplete' THEN 1 ELSE 0 END) AS incomplete_days,
       sum(CASE WHEN f.is_anomaly THEN 1 ELSE 0 END)                   AS anomaly_days
FROM {gold.fact_daily_energy} f
JOIN {gold.dim_building} b ON f.building_id = b.building_id
GROUP BY b.building_id, b.primary_use, b.floor_area_sqm
ORDER BY b.building_id;
