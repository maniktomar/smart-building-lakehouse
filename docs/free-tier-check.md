# Databricks Free Edition: feature check

What the official docs say (read 2026-10-20), and what actually worked in my workspace.
Only the **Verified in my workspace** column counts as evidence; fill it in as you go.

Sources: [Free Edition limitations (Databricks docs)](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations),
[Free Edition limitations (Microsoft Learn)](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-edition-limitations).

| Feature | What the docs say | Verified in my workspace | Notes |
|---|---|---|---|
| Serverless compute (notebooks, jobs) | Only compute type; no custom clusters, no GPUs | ☐ | |
| SQL warehouse | One, 2X-Small only | ☐ | |
| Unity Catalog: `CREATE CATALOG` | Not listed as a limit | ☐ | If it fails, use the default `workspace` catalog |
| Unity Catalog Volumes | Recommended replacement for DBFS/mounts | ☐ | |
| ADLS / custom storage locations | **Not supported** ("custom workspace storage locations") | n/a | We use a Volume instead; stated in README |
| Jobs | Max 5 concurrent job tasks per account | ☐ | |
| Job failure email notifications | Not listed as a limit | ☐ | Tested in M4 |
| Unity Catalog lineage | Not listed as a limit | ☐ | Tested in M4 |
| SQL alerts / dashboards | Not listed as a limit | ☐ | Tested in M4 |
| Databricks Asset Bundles | Not mentioned in the limitations pages | ☐ | Tested in M5 |
| Languages | Python and SQL only (no R, no Scala) | n/a | We only use Python and SQL |
| Fair-usage quota | Compute shuts off for the rest of the day if exceeded | n/a | Keep the data subset small |

## Local development

| Check | Result | Date |
|---|---|---|
| PySpark on native Windows | **Failed**: needs Hadoop `winutils.exe` (`HADOOP_HOME and hadoop.home.dir are unset`) | 2026-10-20 |
| PySpark 4.0.1 + delta-spark 4.0.1 in Docker (`Dockerfile.test`) | **Passed**: Delta write, `MERGE INTO` and time travel (`tests/test_smoke.py`) | 2026-10-20 |
| Full test suite (56 tests) in Docker and on GitHub Actions | **Passed** | 2026-10-21 |
| Full pipeline on real data, local Spark + Delta in Docker | **Passed**: see `docs/run-results.md` | 2026-10-21 |

Nothing in the first table has been verified in a Databricks workspace yet.
