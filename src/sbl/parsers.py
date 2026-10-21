"""Read landing files into a flat DataFrame of string columns.

Every parser returns the source's own columns as strings, plus:
  _source_file      the file each row came from
  _corrupt_record   the raw text of a row Spark could not parse (NULL for good rows)
Keeping values as strings means bronze stores exactly what arrived; typing happens in silver.
"""

from __future__ import annotations

import csv
from collections import defaultdict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from sbl.lakehouse import cache_if_possible

CORRUPT = "_corrupt_record"


def source_file_col():
    """Path of the file a row came from, without the 'file:' / 'dbfs:' scheme.
    `_metadata.file_path` works on Unity Catalog, where input_file_name() is not supported."""
    return F.regexp_replace(F.col("_metadata.file_path"), r"^[a-zA-Z]+:/+", "/")


def read_header(path: str) -> list[str]:
    with open(path, newline="", encoding="utf-8") as f:
        return next(csv.reader(f), [])


def read_csv(spark: SparkSession, files: list[str], src: dict) -> DataFrame:
    """CSV with a header row. Files are grouped by header, so a file with extra columns
    (schema drift) is read with its own schema rather than silently misaligned."""
    groups: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for path in files:
        groups[tuple(read_header(path))].append(path)

    frames = []
    for header, paths in groups.items():
        schema = StructType(
            [StructField(c, StringType()) for c in header] + [StructField(CORRUPT, StringType())]
        )
        df = (
            spark.read.schema(schema)
            .option("header", "true")
            .option("mode", "PERMISSIVE")                 # keep bad rows instead of failing
            .option("columnNameOfCorruptRecord", CORRUPT)
            .csv(paths)
            .withColumn("_source_file", source_file_col())
        )
        frames.append(df)

    out = frames[0]
    for df in frames[1:]:
        out = out.unionByName(df, allowMissingColumns=True)
    return out


def read_open_meteo_json(spark: SparkSession, files: list[str], src: dict) -> DataFrame:
    """One Open-Meteo archive response per file: {"hourly": {"time": [...], "<var>": [...]}}.
    Explodes the parallel arrays into one row per hour."""
    raw = (
        spark.read.option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT)
        .json(files)
        .withColumn("_source_file", source_file_col())
    )
    if CORRUPT not in raw.columns:
        raw = raw.withColumn(CORRUPT, F.lit(None).cast("string"))
    # Spark refuses queries on the corrupt-record column of an uncached raw file read.
    raw = cache_if_possible(raw)

    site_id = F.regexp_extract("_source_file", src["site_id_path_regex"], 1)
    bad = raw.filter(F.col(CORRUPT).isNotNull()).select(
        "_source_file", CORRUPT, site_id.alias("site_id")
    )
    if "hourly" not in raw.columns:
        return bad

    variables = [f.name for f in raw.schema["hourly"].dataType.fields if f.name != "time"]
    good = (
        raw.filter(F.col(CORRUPT).isNull())
        .select("_source_file", "hourly", F.posexplode("hourly.time").alias("pos", "time"))
        .select(
            site_id.alias("site_id"),
            F.col("time").cast("string").alias("time"),
            *[F.col(f"hourly.{v}")[F.col("pos")].cast("string").alias(v) for v in variables],
            "_source_file",
            F.lit(None).cast("string").alias(CORRUPT),
        )
    )
    return good.unionByName(bad, allowMissingColumns=True)


PARSERS = {
    "csv": read_csv,
    "open_meteo_json": read_open_meteo_json,
}
