# Data dictionary: gold layer

Generated from `src/sbl/conf/gold.yml` by `python -m sbl.data_dictionary`. The same text is
written to the tables as table and column comments, so it also shows in Catalog Explorer
and in Power BI.

Star schema: `fact_daily_energy` (building x day) joins `dim_building` on `building_id` and
`dim_date` on `date_key`; `fact_daily_weather` (site x day) joins on `site_id` + `date_key`.

Model parameters: `degree_day_base_c` = 18.0, `min_completeness` = 0.9, `anomaly_z_threshold` = 3.0.

## gold.dim_building

One row per building. Join to fact_daily_energy on building_id.

| column | description |
|---|---|
| `building_id` | Business key of the building (BDG2 building id). |
| `site_id` | Site (campus) the building belongs to. Join to fact_daily_weather on site_id + date. |
| `primary_use` | Primary space usage, e.g. Education, Office. |
| `floor_area_sqm` | Gross floor area in square metres. |
| `year_built` | Year the building was built (null if unknown). |
| `latitude` | Site latitude (BDG2 publishes site-level, approximate coordinates). |
| `longitude` | Site longitude. |
| `timezone` | IANA timezone of the site. All timestamps are local wall-clock time in this zone. |

## gold.dim_date

One row per calendar date covered by the meter data.

| column | description |
|---|---|
| `date_key` | Integer key yyyymmdd. |
| `date` | Calendar date. |
| `year` | Calendar year. |
| `quarter` | Calendar quarter (1-4). |
| `month` | Month number (1-12). |
| `month_name` | Month name, e.g. January. |
| `day_of_week` | Day of week name, e.g. Monday. |
| `is_weekend` | True on Saturday and Sunday. |

## gold.fact_daily_weather

One row per site and date, aggregated from hourly Open-Meteo data.

| column | description |
|---|---|
| `site_id` | Site key. |
| `date_key` | Date key (yyyymmdd). |
| `date` | Calendar date (site-local). |
| `avg_temperature_c` | Mean of hourly temperatures (°C). |
| `min_temperature_c` | Minimum hourly temperature (°C). |
| `max_temperature_c` | Maximum hourly temperature (°C). |
| `heating_degree_days` | max(0, base - avg_temperature_c), base from gold.yml params. |
| `cooling_degree_days` | max(0, avg_temperature_c - base). |
| `hours_observed` | Number of hourly weather observations that day. |

## gold.fact_daily_energy

One row per building and date. Grain of the main Power BI fact table.

| column | description |
|---|---|
| `building_id` | Building key. |
| `site_id` | Site key (denormalised for convenience). |
| `date_key` | Date key (yyyymmdd). |
| `date` | Calendar date (site-local). |
| `energy_kwh` | Total electricity use that day (sum of hourly readings, kWh). |
| `hours_reported` | Number of hourly readings received. |
| `zero_hours` | Number of hourly readings equal to 0 (often a meter outage rather than true zero use). |
| `completeness_pct` | Share of the 24 hours with a non-zero reading, 0-100. |
| `energy_kwh_per_sqm` | energy_kwh / floor_area_sqm. |
| `avg_temperature_c` | Daily mean outdoor temperature at the site (°C). |
| `heating_degree_days` | Heating degree days at the site. |
| `cooling_degree_days` | Cooling degree days at the site. |
| `expected_kwh` | Energy predicted from weather by a per-building linear model (kWh ~ HDD + CDD). Null if not modelled. |
| `weather_normalised_ratio` | energy_kwh / expected_kwh. 1.0 means "as expected for the weather". |
| `residual_z` | (energy_kwh - expected_kwh) / standard deviation of that building's residuals. |
| `data_quality_flag` | ok, or incomplete when completeness is below the gold.yml threshold. |
| `is_anomaly` | True when |residual_z| is above the gold.yml threshold on an ok day. |
