"""Shadow validation: legacy platform (Athena) vs new platform (Snowflake).

During the migration both platforms run in parallel ("shadow mode"). This script
reconciles the critical business metrics computed on each side, per month and
superior body, and only approves the cut-over when every metric reaches the
minimum adherence (default 99.5%).

Checks
------
1. Key coverage: (ano_mes, cod_orgao_superior) pairs present on both sides.
2. Metric adherence, per metric: 1 - sum(|legacy - new|) / sum(|legacy|).
3. Divergence report: the pairs with the largest absolute differences, to guide
   the investigation (missing rows, duplicated loads, casting, filters, timezone).

Backends
--------
    --legacy athena      analytics.legacy_execucao_mensal (Athena, via PyAthena)
    --legacy local       independent DuckDB aggregation over the RAW csv files
    --new snowflake      ANALYTICS.GOLD.AGG_EXECUCAO_MENSAL
    --new local          gold.agg_execucao_mensal in the local DuckDB warehouse

The local/local mode runs in CI: the legacy side re-reads the raw files with its own
parsing logic, so it independently checks the whole pipeline (bronze + dbt).

    python validation/shadow_validation.py --legacy local --new local
    python validation/shadow_validation.py --legacy athena --new snowflake --months 202603 202604
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
METRICS = ["qtd_lancamentos", "valor_empenhado", "valor_liquidado", "valor_pago"]
Key = tuple[str, str]
Rows = dict[Key, dict[str, Decimal]]


# --------------------------------------------------------------------------- backends
def _to_rows(records) -> Rows:
    rows: Rows = {}
    for ano_mes, orgao, *values in records:
        rows[(str(ano_mes), str(orgao).strip())] = {
            metric: Decimal(str(value or 0)) for metric, value in zip(METRICS, values, strict=True)
        }
    return rows


def _month_filter(months: list[str], column: str = "ano_mes") -> str:
    if not months:
        return ""
    return f" where {column} in ({', '.join(repr(m) for m in months)})"


def legacy_local(months: list[str], raw_dir: Path) -> Rows:
    import duckdb

    raw_glob = (raw_dir / "despesas" / "*" / "*.csv").as_posix()
    sql = f"""
        with raw as (
            select
                replace(trim("Ano e mês do lançamento"), '/', '')  as ano_mes,
                trim("Código Órgão Superior")                      as cod_orgao_superior,
                cast(replace(trim("Valor Empenhado (R$)"), ',', '.') as decimal(18,2)) as empenhado,
                cast(replace(trim("Valor Liquidado (R$)"), ',', '.') as decimal(18,2)) as liquidado,
                cast(replace(trim("Valor Pago (R$)"), ',', '.') as decimal(18,2))      as pago
            from read_csv('{raw_glob}', delim=';', header=true, quote='"',
                          encoding='latin-1', all_varchar=true)
        )
        select ano_mes, cod_orgao_superior, count(*),
               sum(coalesce(empenhado, 0)), sum(coalesce(liquidado, 0)), sum(coalesce(pago, 0))
        from raw{_month_filter(months)}
        group by 1, 2
    """
    return _to_rows(duckdb.connect().execute(sql).fetchall())


def new_local(months: list[str], warehouse: Path) -> Rows:
    import duckdb

    sql = (
        "select ano_mes, cod_orgao_superior, " + ", ".join(METRICS)
        + " from gold.agg_execucao_mensal" + _month_filter(months)
    )
    con = duckdb.connect(str(warehouse), read_only=True)
    return _to_rows(con.execute(sql).fetchall())


def legacy_athena(months: list[str]) -> Rows:
    from pyathena import connect

    cursor = connect(
        s3_staging_dir=os.environ["ATHENA_RESULTS_S3"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        work_group=os.environ.get("ATHENA_WORKGROUP", "primary"),
    ).cursor()
    cursor.execute(
        "select ano_mes, cod_orgao_superior, " + ", ".join(METRICS)
        + " from analytics.legacy_execucao_mensal" + _month_filter(months)
    )
    return _to_rows(cursor.fetchall())


def new_snowflake(months: list[str]) -> Rows:
    import snowflake.connector

    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "snowflake_jwt"),
        role="REPORTER",
        warehouse="WH_BI",
        database="ANALYTICS",
        schema="GOLD",
    )
    cursor = con.cursor()
    cursor.execute(
        "select ano_mes, cod_orgao_superior, " + ", ".join(METRICS)
        + " from agg_execucao_mensal" + _month_filter(months)
    )
    return _to_rows(cursor.fetchall())


# ------------------------------------------------------------------------- comparison
@dataclass
class Result:
    coverage: float
    adherence: dict[str, float]
    only_legacy: list[Key]
    only_new: list[Key]
    top_divergences: list[tuple[Key, str, Decimal, Decimal]]

    def passed(self, threshold: float) -> bool:
        return self.coverage >= threshold and all(v >= threshold for v in self.adherence.values())


def compare(legacy: Rows, new: Rows, top: int = 10) -> Result:
    keys = set(legacy) | set(new)
    common = set(legacy) & set(new)
    coverage = len(common) / len(keys) if keys else 1.0

    adherence: dict[str, float] = {}
    divergences = []
    for metric in METRICS:
        base = sum(abs(legacy[k][metric]) for k in keys if k in legacy)
        diff = Decimal(0)
        for key in keys:
            old = legacy.get(key, {}).get(metric, Decimal(0))
            cur = new.get(key, {}).get(metric, Decimal(0))
            if old != cur:
                diff += abs(old - cur)
                divergences.append((key, metric, old, cur))
        adherence[metric] = 1.0 if base == 0 else max(0.0, 1 - float(diff / base))

    divergences.sort(key=lambda item: abs(item[2] - item[3]), reverse=True)
    return Result(
        coverage=coverage,
        adherence=adherence,
        only_legacy=sorted(set(legacy) - set(new)),
        only_new=sorted(set(new) - set(legacy)),
        top_divergences=divergences[:top],
    )


def render_report(result: Result, threshold: float, legacy_name: str, new_name: str) -> str:
    status = "APPROVED" if result.passed(threshold) else "REJECTED"
    lines = [
        f"# Shadow validation — {legacy_name} vs {new_name}",
        "",
        f"- Run at: {datetime.now():%Y-%m-%d %H:%M}",
        f"- Minimum adherence: {threshold:.2%}",
        f"- **Result: {status}**",
        "",
        "| Check | Adherence |",
        "|---|---|",
        f"| Key coverage (month x superior body) | {result.coverage:.4%} |",
        *[f"| {metric} | {value:.4%} |" for metric, value in result.adherence.items()],
        "",
    ]
    if result.only_legacy or result.only_new:
        lines += [
            "## Missing keys",
            f"- Only in legacy: {result.only_legacy[:20]}",
            f"- Only in new: {result.only_new[:20]}",
            "",
        ]
    if result.top_divergences:
        lines += [
            "## Largest divergences",
            "",
            "| ano_mes | órgão | metric | legacy | new | diff |",
            "|---|---|---|---|---|---|",
        ]
        for (ano_mes, orgao), metric, old, cur in result.top_divergences:
            lines.append(f"| {ano_mes} | {orgao} | {metric} | {old} | {cur} | {cur - old} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--legacy", choices=["local", "athena"], default="local")
    parser.add_argument("--new", choices=["local", "snowflake"], default="local")
    parser.add_argument("--months", nargs="*", default=[])
    parser.add_argument("--threshold", type=float, default=0.995)
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "data" / "raw")
    parser.add_argument("--warehouse", type=Path, default=ROOT / "data" / "warehouse.duckdb")
    parser.add_argument("--report", type=Path, default=ROOT / "validation" / "reports" / "latest.md")
    args = parser.parse_args(argv)

    legacy = legacy_local(args.months, args.raw_dir) if args.legacy == "local" else legacy_athena(args.months)
    new = new_local(args.months, args.warehouse) if args.new == "local" else new_snowflake(args.months)

    result = compare(legacy, new)
    report = render_report(result, args.threshold, args.legacy, args.new)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report, encoding="utf-8")
    print(report)
    return 0 if result.passed(args.threshold) else 1


if __name__ == "__main__":
    sys.exit(main())
