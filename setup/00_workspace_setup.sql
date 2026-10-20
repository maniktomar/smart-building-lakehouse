-- M0: create the Unity Catalog objects the pipeline needs.
-- Run this in the Databricks SQL editor (or a notebook with %sql) on serverless compute.
-- Every statement is idempotent (IF NOT EXISTS), so re-running it is safe.

-- 1. Catalog: the top level of the Unity Catalog namespace (catalog.schema.table).
--    If this fails with a permission error, Free Edition is not letting you create
--    catalogs: skip it, use the default `workspace` catalog instead, replace
--    `smart_building` below with `workspace`, and record that in docs/free-tier-check.md.
CREATE CATALOG IF NOT EXISTS smart_building
  COMMENT 'Smart Building Energy and Supply Lakehouse (portfolio project)';

USE CATALOG smart_building;

-- 2. One schema per medallion layer, plus `ops` for audit, quality and quarantine tables.
CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw, append-only copies of source data plus ingestion metadata';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned, typed, deduplicated data, upserted with MERGE';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Curated, business-ready star schema for Power BI';
CREATE SCHEMA IF NOT EXISTS ops    COMMENT 'Pipeline audit log, data-quality results and quarantined records';

-- 3. A managed Volume: governed file storage inside Unity Catalog.
--    Raw CSV/JSON files land here before bronze ingestion. Stands in for ADLS.
CREATE VOLUME IF NOT EXISTS bronze.landing
  COMMENT 'Landing zone for raw source files (stands in for ADLS on Free Edition)';

-- 4. Check: you should see four schemas and one volume.
SHOW SCHEMAS IN smart_building;
SHOW VOLUMES IN smart_building.bronze;
