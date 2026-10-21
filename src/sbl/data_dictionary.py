"""Generate docs/data-dictionary.md from gold.yml, the same descriptions written to the tables.

    python -m sbl.data_dictionary > docs/data-dictionary.md
"""

from __future__ import annotations

from sbl.config import load_config


def render() -> str:
    gold = load_config("local").gold
    out = [
        "# Data dictionary: gold layer",
        "",
        "Generated from `src/sbl/conf/gold.yml` by `python -m sbl.data_dictionary`. The same text is",
        "written to the tables as table and column comments, so it also shows in Catalog Explorer",
        "and in Power BI.",
        "",
        "Star schema: `fact_daily_energy` (building x day) joins `dim_building` on `building_id` and",
        "`dim_date` on `date_key`; `fact_daily_weather` (site x day) joins on `site_id` + `date_key`.",
        "",
        "Model parameters: " + ", ".join(f"`{k}` = {v}" for k, v in gold["params"].items()) + ".",
    ]
    for table, spec in gold["tables"].items():
        out += ["", f"## gold.{table}", "", spec["description"], "", "| column | description |",
                "|---|---|"]
        out += [f"| `{col}` | {text} |" for col, text in spec["columns"].items()]
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    print(render(), end="")
