"""Download Building Data Genome Project 2 files and cut the subset this project uses.

    python scripts/prepare_bdg2_subset.py

What it does (and does not do):
  - downloads metadata.csv and the raw electricity.csv (174 MB) into data/raw/bdg2/ if missing
  - keeps 8 buildings from two sites (see SUBSET below), 2016-2017, electricity only
  - reshapes the wide file (one column per building) into long rows
    (timestamp, building_id, meter, meter_reading), one CSV per month, in data/landing/meters/
  - writes the 8 metadata rows, all original columns, to data/landing/buildings/
  - does NOT clean anything: missing readings stay as empty values, so bronze sees the real gaps

Simplification: in production the source system would deliver these monthly files; here the
script plays that role. BDG2 is licensed CC BY-SA 4.0 (see docs/data-sources.md).
"""

from __future__ import annotations

import argparse
import hashlib
import os
from urllib.request import urlretrieve

import pandas as pd

BASE_URL = "https://media.githubusercontent.com/media/buds-lab/building-data-genome-project-2/master"
FILES = {
    "metadata.csv": f"{BASE_URL}/data/metadata/metadata.csv",
    "electricity.csv": f"{BASE_URL}/data/meters/raw/electricity.csv",
}

# Two sites with opposite climates: Panther (Orlando, cooling-driven) and Lamb (Cardiff,
# heating-driven). Four buildings each, picked for different space uses and few gaps.
SUBSET = [
    "Panther_education_Annetta", "Panther_office_Ruthie",
    "Panther_lodging_Jana", "Panther_retail_Romeo",
    "Lamb_education_Domitila", "Lamb_education_Luz",
    "Lamb_health_Ken", "Lamb_office_Caitlin",
]


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw-dir", default="data/raw/bdg2")
    parser.add_argument("--landing", default="data/landing")
    args = parser.parse_args()

    os.makedirs(args.raw_dir, exist_ok=True)
    for name, url in FILES.items():
        path = os.path.join(args.raw_dir, name)
        if not os.path.exists(path):
            print(f"downloading {url}")
            urlretrieve(url, path)
        print(f"{name}: sha256 {sha256(path)}")

    meta = pd.read_csv(os.path.join(args.raw_dir, "metadata.csv"), dtype=str)
    meta = meta[meta["building_id"].isin(SUBSET)]
    os.makedirs(os.path.join(args.landing, "buildings"), exist_ok=True)
    meta.to_csv(os.path.join(args.landing, "buildings", "bdg2_metadata_subset.csv"), index=False)
    print(f"buildings: {len(meta)} rows")

    # Read as strings so values are written back exactly as published.
    wide = pd.read_csv(os.path.join(args.raw_dir, "electricity.csv"),
                       usecols=["timestamp", *SUBSET], dtype=str, keep_default_na=False)
    long = wide.melt(id_vars="timestamp", var_name="building_id", value_name="meter_reading")
    long.insert(2, "meter", "electricity")

    out_dir = os.path.join(args.landing, "meters")
    os.makedirs(out_dir, exist_ok=True)
    long["month"] = long["timestamp"].str[:7]
    for month, part in long.groupby("month"):
        part.drop(columns="month").to_csv(os.path.join(out_dir, f"electricity_{month}.csv"),
                                          index=False)
    empty = (long["meter_reading"].str.strip() == "").sum()
    print(f"meter readings: {len(long)} rows in {long['month'].nunique()} monthly files "
          f"({empty} empty readings left in place)")


if __name__ == "__main__":
    main()
