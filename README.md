# Smart Building Energy and Supply Lakehouse

A medallion (bronze / silver / gold) lakehouse on Databricks with Delta Lake, PySpark and SQL:
smart-building energy meter data plus weather data, with data quality, reconciliation tests and CI/CD.

> **Status: M0 (setup).** Nothing has been ingested yet. This README is filled in milestone by milestone
> and only describes what has actually run.

## Repository layout

| Path | Purpose |
|---|---|
| `conf/` | YAML config: catalog, schemas, tables, keys, expectations |
| `src/sbl/` | Reusable pipeline code (Python package) |
| `setup/` | One-off workspace setup SQL |
| `tests/` | pytest unit, integration and reconciliation tests |
| `docs/` | Data sources, free-tier check, runbook, data dictionary |

## Running the tests locally

PySpark does not run on native Windows without extra Hadoop binaries, so the tests run in a small Linux container:

```bash
docker build -f Dockerfile.test -t sbl-test .
docker run --rm sbl-test
```

## Data sources

See [docs/data-sources.md](docs/data-sources.md). Building Data Genome Project 2 (CC BY-SA 4.0) and Open-Meteo (CC BY 4.0).
