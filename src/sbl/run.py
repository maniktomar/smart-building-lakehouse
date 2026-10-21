"""Pipeline entry point (`sbl-run`). Runs one stage from pipeline.yml, or all of them in order.

    sbl-run --env local --stage all
    sbl-run --env databricks --stage bronze --catalog workspace

Any failure raises, which exits non-zero: the Databricks job task fails and sends its alert.
"""

from __future__ import annotations

import argparse
import sys

from sbl import bronze, extract, gold, quality, reconcile, silver
from sbl.audit import new_run_id
from sbl.config import ConfigError, load_config, validate
from sbl.lakehouse import Lakehouse
from sbl.spark import get_spark


def run_stage(lh: Lakehouse, cfg, stage: dict, run_id: str, reset_watermark: bool = False) -> None:
    print(f"== stage {stage['name']} ==")
    for source in stage.get("bronze", []) or []:
        bronze.ingest_source(lh, source, cfg.sources[source], run_id)
    for table in stage.get("silver", []) or []:
        silver.build_table(lh, table, cfg.silver[table], run_id, reset_watermark)
    for source in stage.get("extract", []) or []:
        extract.extract_weather(lh, source, cfg.sources[source], run_id)
    if stage.get("gold"):
        gold.build_all(lh, cfg.gold, run_id)
    if stage.get("quality"):
        quality.run_rules(lh, cfg.quality, run_id)
    if stage.get("reconcile"):
        reconcile.run(lh, cfg, run_id)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", default="local", help="config in sbl/conf/env/<env>.yml")
    parser.add_argument("--stage", default="all", help="a stage name from pipeline.yml, or 'all'")
    parser.add_argument("--catalog", help="override the Unity Catalog catalog name")
    parser.add_argument("--landing-root", help="override the landing folder or Volume path")
    parser.add_argument("--run-id", help="batch id to use (default: generated)")
    parser.add_argument("--reset-watermark", action="store_true",
                        help="silver: reprocess all bronze rows (MERGE keeps this idempotent)")
    args = parser.parse_args(argv)

    cfg = load_config(args.env, catalog=args.catalog, landing_root=args.landing_root)
    problems = validate(cfg)
    if problems:
        raise ConfigError("invalid config:\n  " + "\n  ".join(problems))

    stages = cfg.stages if args.stage == "all" else [s for s in cfg.stages if s["name"] == args.stage]
    if not stages:
        parser.error(f"unknown stage {args.stage!r}; choose from "
                     f"{[s['name'] for s in cfg.stages]} or 'all'")

    lh = Lakehouse(get_spark(args.env), cfg.env)
    lh.ensure_schemas()
    run_id = args.run_id or new_run_id()
    print(f"run_id={run_id} env={args.env} stage={args.stage}")
    for stage in stages:
        run_stage(lh, cfg, stage, run_id, args.reset_watermark)
    print(f"run {run_id} finished OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
