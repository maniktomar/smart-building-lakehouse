"""One place that knows where tables live.

On Databricks, tables are Unity Catalog names: smart_building.silver.meter_readings.
Locally, there is no Unity Catalog, so tables are Delta folders: data/lakehouse/silver/meter_readings.
Every other module calls these helpers, so the same pipeline code runs in both places.
"""

from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


class Lakehouse:
    def __init__(self, spark: SparkSession, env: dict):
        self.spark = spark
        self.catalog = env.get("catalog")
        self.schemas = env["schemas"]
        root = env.get("storage_root")
        self.storage_root = os.path.abspath(root) if root and not self.catalog else None
        landing = env["landing_root"]
        self.landing_root = landing if landing.startswith("/Volumes") else os.path.abspath(landing)

    # ---- naming -------------------------------------------------------------------------
    def name(self, layer: str, table: str) -> str:
        """Unity Catalog three-part name (Databricks only)."""
        return f"{self.catalog}.{self.schemas[layer]}.{table}"

    def path(self, layer: str, table: str) -> str:
        """Folder of a path-based Delta table (local only)."""
        return f"{self.storage_root}/{self.schemas[layer]}/{table}"

    def ref(self, layer: str, table: str) -> str:
        """Identifier usable in SQL text, e.g. in ALTER TABLE or RESTORE."""
        if self.catalog:
            return self.name(layer, table)
        return f"delta.`{self.path(layer, table)}`"

    @staticmethod
    def split(qualified: str) -> tuple[str, str]:
        """'silver.meter_readings' -> ('silver', 'meter_readings')."""
        layer, table = qualified.split(".", 1)
        return layer, table

    # ---- setup --------------------------------------------------------------------------
    def ensure_schemas(self) -> None:
        if not self.catalog:
            return  # local folders are created on first write
        for schema in self.schemas.values():
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{schema}")

    # ---- read / write -------------------------------------------------------------------
    def exists(self, layer: str, table: str) -> bool:
        if self.catalog:
            return self.spark.catalog.tableExists(self.name(layer, table))
        return DeltaTable.isDeltaTable(self.spark, self.path(layer, table))

    def read(self, layer: str, table: str) -> DataFrame:
        if self.catalog:
            return self.spark.read.table(self.name(layer, table))
        return self.spark.read.format("delta").load(self.path(layer, table))

    def delta(self, layer: str, table: str) -> DeltaTable:
        if self.catalog:
            return DeltaTable.forName(self.spark, self.name(layer, table))
        return DeltaTable.forPath(self.spark, self.path(layer, table))

    def write(self, df: DataFrame, layer: str, table: str, mode: str = "append",
              merge_schema: bool = False) -> None:
        writer = df.write.format("delta").mode(mode)
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        if self.catalog:
            writer.saveAsTable(self.name(layer, table))
        else:
            writer.save(self.path(layer, table))

    def sql(self, statement: str) -> DataFrame:
        return self.spark.sql(statement)

    # ---- documentation ------------------------------------------------------------------
    def set_comments(self, layer: str, table: str, description: str,
                     columns: dict[str, str] | None = None) -> None:
        """Write table and column descriptions (shown in Catalog Explorer and Power BI)."""
        ref = self.ref(layer, table)
        esc = lambda s: s.replace("'", "\\'")  # noqa: E731
        if self.catalog:
            self.sql(f"COMMENT ON TABLE {ref} IS '{esc(description)}'")
        else:
            # Path-based tables have no catalog entry; store the description as a table property.
            self.sql(f"ALTER TABLE {ref} SET TBLPROPERTIES ('comment' = '{esc(description)}')")
        existing = set(self.read(layer, table).columns)
        for col, text in (columns or {}).items():
            if col in existing:
                self.sql(f"ALTER TABLE {ref} ALTER COLUMN `{col}` COMMENT '{esc(text)}'")


def cache_if_possible(df: DataFrame) -> DataFrame:
    """Cache locally; serverless compute does not support the caching API, so skip it there."""
    try:
        return df.cache()
    except Exception:
        return df
