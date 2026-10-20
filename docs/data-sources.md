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
- **Subset:** decided in M1 (one or two sites, a handful of buildings, electricity only).

## Source B: Open-Meteo Historical Weather API (API)

- **What:** hourly historical weather (temperature etc.) by latitude/longitude and date range.
- **Where:** <https://open-meteo.com/>
- **Licence and terms (checked 2026-10-20):** the data is **CC BY 4.0**. The free API is for **non-commercial use only**,
  limited to fewer than 10,000 calls a day, 5,000 an hour and 600 a minute. A portfolio project counts as non-commercial.
- **How downloaded:** decided in M1.

## Source C: supply chain (optional)

Not included yet; decided after M3.
