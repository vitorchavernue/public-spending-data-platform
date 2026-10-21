from decimal import Decimal

import duckdb

from common.schema import BRONZE_COLUMNS, COLUMN_MAP
from local.build_bronze import build

HEADER = ";".join(f'"{name}"' for name in COLUMN_MAP)


def write_raw(raw_dir, ano_mes, values):
    folder = raw_dir / "despesas" / f"ano_mes={ano_mes}"
    folder.mkdir(parents=True)
    line = ";".join(f'"{v}"' for v in values)
    (folder / f"{ano_mes}_Despesas.csv").write_bytes(f"{HEADER}\r\n{line}\r\n".encode("latin-1"))


def test_bronze_schema_types_and_amounts(tmp_path):
    values = ["texto"] * len(COLUMN_MAP)
    values[0] = "2026/04"
    for position in range(41, 47):          # amount columns
        values[position] = "-1234,56"
    values[23] = " SP "                     # uf with spaces
    write_raw(tmp_path / "raw", "202604", values)

    assert build(tmp_path / "raw", tmp_path / "bronze") == 1

    con = duckdb.connect()
    table = f"read_parquet('{(tmp_path / 'bronze' / 'despesas').as_posix()}/*/*.parquet', hive_partitioning=true)"
    columns = [row[0] for row in con.execute(f"describe select * from {table}").fetchall()]
    assert set(BRONZE_COLUMNS) <= set(columns)

    ano_mes, uf, empenhado, pago = con.execute(
        f"select cast(ano_mes as varchar), uf, valor_empenhado, valor_pago from {table}"
    ).fetchone()
    assert ano_mes == "202604"
    assert uf == "SP"
    assert empenhado == pago == Decimal("-1234.56")
