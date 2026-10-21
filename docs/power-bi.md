# Power BI guide

**Status: not done.** This guide was written but not followed end to end: there is no Power BI
report in this repository. It describes how the gold layer is meant to be consumed.

## Model

Load the four gold tables and create these relationships (single direction, many-to-one):

| From (many) | To (one) | Columns |
|---|---|---|
| `fact_daily_energy` | `dim_building` | `building_id` |
| `fact_daily_energy` | `dim_date` | `date_key` |
| `fact_daily_weather` | `dim_date` | `date_key` |

`fact_daily_weather` is per site, not per building. To show weather next to energy, either use the
weather columns already copied onto `fact_daily_energy` (`avg_temperature_c`, degree days), or add a
small `dim_site` (distinct `site_id` from `dim_building`) and relate both facts to it.

Mark `dim_date` as the date table (Table tools > Mark as date table > `date`).

## Suggested measures (DAX)

```dax
Energy kWh            = SUM ( fact_daily_energy[energy_kwh] )
Floor area m²         = SUM ( dim_building[floor_area_sqm] )
Energy kWh per m²     = DIVIDE ( [Energy kWh], [Floor area m²] )
Expected kWh          = SUM ( fact_daily_energy[expected_kwh] )
Weather-normalised %  = DIVIDE ( [Energy kWh], [Expected kWh] )
Anomaly days          = CALCULATE ( COUNTROWS ( fact_daily_energy ), fact_daily_energy[is_anomaly] )
Incomplete days       = CALCULATE ( COUNTROWS ( fact_daily_energy ),
                                    fact_daily_energy[data_quality_flag] = "incomplete" )
```

`Energy kWh per m²` over a period divides by floor area once per building, which is right because
floor area lives on the dimension, not the fact.

## Connecting

**Databricks (needs a workspace and Power BI Desktop on Windows):**
1. In Databricks, open SQL Warehouses > your warehouse > Connection details.
2. Power BI Desktop > Get data > Azure Databricks (works for Databricks on any cloud).
3. Enter the Server hostname and HTTP path, sign in, choose **DirectQuery** or **Import**
   (Import is fine for this size), and select the `gold` schema tables.
4. Or use Databricks "Partner Connect" / "Publish to Power BI" from Catalog Explorer, where offered.

Column descriptions from `gold.yml` are stored as Unity Catalog comments and show as field
descriptions in Power BI.

**Without Databricks:** the local gold tables are Delta folders under `data/lakehouse/gold/`.
Power BI cannot read Delta folders directly without extra connectors; the simple route is to export
them to CSV or Parquet from Spark and import those.
