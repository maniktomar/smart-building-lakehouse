# Data sources and licences

## Source A: Building Data Genome Project 2 (files)

- **What:** hourly meter readings (2016 and 2017) and building metadata for about 1,636 non-residential buildings.
- **Where:** <https://github.com/buds-lab/building-data-genome-project-2>. CSV files are stored with Git LFS
  and served from `https://media.githubusercontent.com/media/buds-lab/building-data-genome-project-2/master/<path>`.
- **Files used:**
  - `data/metadata/metadata.csv` (272 KB, 1,636 buildings; includes floor area `sqm`, `lat`/`lng`, `timezone`)
  - `data/meters/raw/electricity.csv` (174 MB, wide format: one `timestamp` column plus one column per building)
- **Licence:** Creative Commons **Attribution-ShareAlike 4.0** (the `LICENSE` file in the repository, checked 2026-10-20).
  Attribution is required, and anything we redistribute from it (for example a sample CSV committed to this repo)
  must be shared under the same licence.
- **Checksums of the files used (sha256, downloaded 2026-10-21):**
  - `metadata.csv`: `992d0b29f24f96ad4332bc4dbb534b7bdd7dd2689aad093f94e93068ecddca02`
  - `electricity.csv`: `039d909d8981e2d69eaeb366144e6ab7e84fa5e7e216aee42bddd95384a66418`
- **Subset:** 8 buildings, electricity only, 2016-01-01 to 2017-12-31 (17,544 hours each):
  - Panther (Orlando area, US/Eastern; cooling-driven): `Panther_education_Annetta`, `Panther_office_Ruthie`,
    `Panther_lodging_Jana`, `Panther_retail_Romeo`
  - Lamb (Cardiff area, Europe/London; heating-driven): `Lamb_education_Domitila`, `Lamb_education_Luz`,
    `Lamb_health_Ken`, `Lamb_office_Caitlin`
- **How downloaded:** `python scripts/prepare_bdg2_subset.py` downloads both files, keeps the subset and writes
  one long-format CSV per month (`timestamp, building_id, meter, meter_reading`) to `data/landing/meters/`, plus the
  8 metadata rows to `data/landing/buildings/`. It reshapes only; it does not clean. 40 readings are empty in the
  source and are left empty, so bronze quarantines them.
- **Observations:** about 19% of each Panther building's hours are exactly 0 (meter outages, most likely). Site
  `Wolf` lists longitude +6.26 with timezone Europe/Dublin and latitude 53.35, which looks like a sign error for
  Dublin (-6.26); that site is not used.

## Source B: Open-Meteo Historical Weather API (API)

- **What:** hourly historical weather (temperature etc.) by latitude/longitude and date range.
- **Where:** <https://open-meteo.com/>
- **Licence and terms (checked 2026-10-20):** the data is **CC BY 4.0**. The free API is for **non-commercial use only**,
  limited to fewer than 10,000 calls a day, 5,000 an hour and 600 a minute. A portfolio project counts as non-commercial.
- **How downloaded:** the pipeline's `extract_weather` stage (`src/sbl/extract.py`) calls the archive API once per
  site and month (48 calls for 2 sites × 24 months) with `hourly=temperature_2m,relative_humidity_2m` and the
  site's timezone, and saves each response unchanged to `landing/weather/site_id=<site>/<YYYY-MM>.json`.
  Site coordinates come from BDG2 metadata (site-level and approximate).
- **Attribution:** Weather data by [Open-Meteo.com](https://open-meteo.com/), CC BY 4.0.

## Source C: supply chain (optional)

Not included. No public supply-chain dataset with a clear licence was chosen, and the project is complete
without it. The project title keeps "Supply" from the original brief; the data does not cover supply chain.
