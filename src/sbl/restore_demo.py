"""Restorability demo: a deliberate bad load into silver, caught by a quality rule, undone with RESTORE.

The bad load imitates a real failure mode: a re-delivered month of readings in Wh instead
of kWh (1000x too large), upserted by the normal MERGE. Steps:
  1. note the current table version and totals
  2. MERGE the bad batch                       -> rows are overwritten with wrong values
  3. run the reading_kwh_range rule            -> it catches the bad rows
  4. RESTORE TABLE ... TO VERSION AS OF <1>    -> Delta time travel puts the old files back
  5. check the totals match step 1 exactly

    python -m sbl.restore_demo --env local
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from sbl.audit import audited, new_run_id
from sbl.config import load_config
from sbl.lakehouse import Lakehouse
from sbl.quality import evaluate
from sbl.spark import get_spark


def table_stats(lh: Lakehouse) -> tuple[int, float]:
    row = lh.read("silver", "meter_readings").agg(F.count("*"), F.sum("reading_kwh")).first()
    return int(row[0]), float(row[1])


def demo(lh: Lakehouse, cfg, run_id: str, month: str = "2017-01") -> dict:
    range_rule = next(r for r in cfg.quality["rules"]["silver.meter_readings"]
                      if r["type"] == "range")
    ref = lh.ref("silver", "meter_readings")
    table = lh.delta("silver", "meter_readings")

    with audited(lh, run_id, "restore", "silver.meter_readings") as audit:
        good_version = table.history(1).select("version").first()[0]
        before = table_stats(lh)

        current = lh.read("silver", "meter_readings")
        building = current.select(F.min("building_id")).first()[0]
        bad = (current.filter((F.col("building_id") == building)
                              & (F.date_format("reading_ts", "yyyy-MM") == month))
               .withColumn("reading_kwh", F.col("reading_kwh") * 1000)
               .withColumn("_batch_id", F.lit(f"{run_id}-BAD"))
               .withColumn("_source_ingested_at", F.current_timestamp()))
        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in cfg.silver["meter_readings"]["keys"])
        table.alias("t").merge(bad.alias("s"), on).whenMatchedUpdateAll().execute()
        bad_rows = bad.count()
        after_bad = table_stats(lh)

        failed_rows, total_rows, _ = evaluate(lh, lh.read("silver", "meter_readings"), range_rule)

        lh.sql(f"RESTORE TABLE {ref} TO VERSION AS OF {good_version}")
        after_restore = table_stats(lh)
        restored_ok = after_restore == before

        history = [(r.version, r.operation) for r in table.history(3).collect()]
        audit.rows_read = bad_rows
        audit.rows_written = bad_rows
        audit.message = (
            f"bad MERGE of {bad_rows} rows ({building}, {month}, x1000); "
            f"range rule caught {failed_rows} rows; restored to version {good_version}; "
            f"kWh before={before[1]:.3f} bad={after_bad[1]:.3f} restored={after_restore[1]:.3f}"
        )
        if not restored_ok:
            raise AssertionError(f"restore did not return the table to {before}: {after_restore}")

    result = {"good_version": good_version, "building": building, "month": month,
              "bad_rows": bad_rows, "rule_failed_rows": failed_rows, "rows_checked": total_rows,
              "before": before, "after_bad_load": after_bad, "after_restore": after_restore,
              "history": history}
    for key, value in result.items():
        print(f"{key:>16}: {value}")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default="local")
    parser.add_argument("--catalog")
    args = parser.parse_args(argv)
    cfg = load_config(args.env, catalog=args.catalog)
    lh = Lakehouse(get_spark(args.env), cfg.env)
    demo(lh, cfg, new_run_id())
    return 0


if __name__ == "__main__":
    sys.exit(main())
