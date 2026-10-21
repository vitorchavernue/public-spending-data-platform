"""Local equivalent of the Glue job ``glue/jobs/raw_to_bronze.py`` using DuckDB.

Produces the same bronze schema (Parquet partitioned by ``ano_mes``) from the
raw zone, so the dbt models, tests and shadow validation can run on a laptop
or in CI without any cloud account.

    python local/build_bronze.py --raw ./data/raw --bronze ./data/bronze
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.schema import AMOUNT_COLUMNS, COLUMN_MAP, RAW_ENCODING, RAW_SEPARATOR  # noqa: E402


def bronze_select(raw_glob: str) -> str:
    columns = []
    for raw_name, bronze_name in COLUMN_MAP.items():
        value = f'trim("{raw_name}")'
        if bronze_name in AMOUNT_COLUMNS:
            value = f"cast(replace({value}, ',', '.') as decimal(18,2))"
        elif bronze_name == "ano_mes":
            value = f"replace({value}, '/', '')"
        columns.append(f"{value} as {bronze_name}")
    columns.append("regexp_extract(filename, '([^/\\\\]+)$', 1) as _source_file")
    columns.append("current_timestamp as _ingested_at")
    return (
        "select\n    "
        + ",\n    ".join(columns)
        + f"\nfrom read_csv('{raw_glob}', delim='{RAW_SEPARATOR}', header=true, quote='\"',"
        f" encoding='{RAW_ENCODING}', all_varchar=true, filename=true)"
    )


def build(raw_dir: Path, bronze_dir: Path) -> int:
    raw_glob = (raw_dir / "despesas" / "*" / "*.csv").as_posix()
    shutil.rmtree(bronze_dir / "despesas", ignore_errors=True)
    (bronze_dir / "despesas").mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(
        f"copy ({bronze_select(raw_glob)}) to '{(bronze_dir / 'despesas').as_posix()}'"
        " (format parquet, partition_by (ano_mes), compression snappy)"
    )
    rows = con.execute(
        f"select count(*) from read_parquet('{(bronze_dir / 'despesas').as_posix()}/*/*.parquet')"
    ).fetchone()[0]
    print(f"bronze rows: {rows}")
    return rows


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, default=Path("data/raw"))
    parser.add_argument("--bronze", type=Path, default=Path("data/bronze"))
    args = parser.parse_args(argv)
    build(args.raw, args.bronze)


if __name__ == "__main__":
    main()
