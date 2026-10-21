"""Load and validate the YAML configuration in sbl/conf/.

All tables, keys, types, rules and paths live in config; the pipeline code reads them from here.
`validate()` is run by CI and at the start of every pipeline run, so a broken config fails
fast with a readable message instead of half-way through a Spark job.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

CONF_DIR = Path(__file__).parent / "conf"

TYPES = {"string", "double", "int", "timestamp"}
FORMATS = {"csv", "open_meteo_json"}
SEVERITIES = {"error", "warn"}
RULE_TYPES = {
    "not_null", "unique", "range", "accepted_values", "references",
    "row_count_min", "max_share", "freshness",
}
LAYERS = ("bronze", "silver", "gold", "ops")


class ConfigError(ValueError):
    """Raised when the configuration is invalid."""


@dataclass
class Config:
    env: dict[str, Any]
    sources: dict[str, dict]
    silver: dict[str, dict]
    gold: dict[str, Any]
    quality: dict[str, Any]
    stages: list[dict]


def _load(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(env_name: str = "local", conf_dir: Path = CONF_DIR, **overrides) -> Config:
    """Read every config file. `overrides` replace top-level env keys (e.g. catalog=...)."""
    env = _load(conf_dir / "env" / f"{env_name}.yml")
    env.update({k: v for k, v in overrides.items() if v is not None})
    # e.g. /Volumes/{catalog}/bronze/landing follows a --catalog override
    env["landing_root"] = str(env.get("landing_root", "")).format(catalog=env.get("catalog"))
    return Config(
        env=env,
        sources=_load(conf_dir / "sources.yml")["sources"],
        silver=_load(conf_dir / "silver.yml")["silver"],
        gold=_load(conf_dir / "gold.yml"),
        quality=_load(conf_dir / "quality.yml"),
        stages=_load(conf_dir / "pipeline.yml")["stages"],
    )


def known_tables(cfg: Config) -> set[str]:
    """Every '<layer>.<table>' name the pipeline creates."""
    tables = {f"bronze.{s['bronze_table']}" for s in cfg.sources.values()}
    tables |= {f"silver.{name}" for name in cfg.silver}
    tables |= {f"gold.{name}" for name in cfg.gold.get("tables", {})}
    return tables


def validate(cfg: Config) -> list[str]:
    """Return a list of problems; an empty list means the config is valid."""
    errors: list[str] = []

    for layer in LAYERS:
        if layer not in cfg.env.get("schemas", {}):
            errors.append(f"env: schemas.{layer} is missing")
    if not cfg.env.get("landing_root"):
        errors.append("env: landing_root is missing")
    if not cfg.env.get("catalog") and not cfg.env.get("storage_root"):
        errors.append("env: set either catalog (Unity Catalog) or storage_root (local paths)")

    for name, src in cfg.sources.items():
        where = f"sources.{name}"
        for key in ("format", "landing_dir", "file_glob", "bronze_table", "columns"):
            if key not in src:
                errors.append(f"{where}: '{key}' is missing")
        if src.get("format") not in FORMATS:
            errors.append(f"{where}: format must be one of {sorted(FORMATS)}")
        if src.get("schema_drift", {}).get("new_columns") not in ("add", "fail"):
            errors.append(f"{where}: schema_drift.new_columns must be 'add' or 'fail'")
        for col in src.get("columns", []):
            if col.get("type") not in TYPES:
                errors.append(f"{where}.{col.get('name')}: type must be one of {sorted(TYPES)}")
            if col.get("type") == "timestamp" and not col.get("format"):
                errors.append(f"{where}.{col.get('name')}: timestamp columns need a format")

    for name, tbl in cfg.silver.items():
        where = f"silver.{name}"
        src = cfg.sources.get(tbl.get("source"))
        if src is None:
            errors.append(f"{where}: source '{tbl.get('source')}' is not defined in sources.yml")
            continue
        bronze_cols = {c["name"] for c in src["columns"]}
        for col, spec in tbl.get("columns", {}).items():
            if spec.get("type") not in TYPES:
                errors.append(f"{where}.{col}: type must be one of {sorted(TYPES)}")
            if spec.get("from") not in bronze_cols:
                errors.append(f"{where}.{col}: 'from: {spec.get('from')}' is not a configured "
                              f"column of source '{tbl['source']}'")
        for key in tbl.get("keys", []):
            if key not in tbl.get("columns", {}):
                errors.append(f"{where}: key '{key}' is not one of its columns")
        if not tbl.get("keys"):
            errors.append(f"{where}: keys are required for MERGE")

    tables = known_tables(cfg)
    for table, rules in cfg.quality.get("rules", {}).items():
        if table not in tables:
            errors.append(f"quality.{table}: unknown table")
        for rule in rules:
            where = f"quality.{table}.{rule.get('name')}"
            if rule.get("type") not in RULE_TYPES:
                errors.append(f"{where}: type must be one of {sorted(RULE_TYPES)}")
            if rule.get("severity") not in SEVERITIES:
                errors.append(f"{where}: severity must be 'error' or 'warn'")
            if rule.get("type") == "references" and rule.get("ref_table") not in tables:
                errors.append(f"{where}: ref_table '{rule.get('ref_table')}' is unknown")

    stage_targets = {"bronze": set(cfg.sources), "silver": set(cfg.silver), "extract": set(cfg.sources)}
    for stage in cfg.stages:
        for kind, valid in stage_targets.items():
            for target in stage.get(kind, []) or []:
                if target not in valid:
                    errors.append(f"pipeline.{stage['name']}: unknown {kind} target '{target}'")

    return errors


def main() -> int:
    """CLI: `sbl-validate-config` checks every environment's config. Used in CI."""
    failed = False
    for env_file in sorted((CONF_DIR / "env").glob("*.yml")):
        problems = validate(load_config(env_file.stem))
        status = "OK" if not problems else f"{len(problems)} problem(s)"
        print(f"[{env_file.stem}] {status}")
        for p in problems:
            print(f"  - {p}")
        failed |= bool(problems)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
