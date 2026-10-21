"""Create the Spark session for the current environment."""

from __future__ import annotations

from pyspark.sql import SparkSession


def local_spark(warehouse_dir: str | None = None, app_name: str = "sbl") -> SparkSession:
    """A small local Spark session with Delta Lake enabled (tests and local runs)."""
    from delta import configure_spark_with_delta_pip  # only needed outside Databricks

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        # Small data: keep shuffles cheap and the UI off so runs start fast.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def get_spark(env_name: str) -> SparkSession:
    if env_name == "local":
        return local_spark()
    # On Databricks the runtime already provides a session with Delta and Unity Catalog.
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark
