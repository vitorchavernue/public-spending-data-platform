"""Report the Athena queries that scanned the most data (migration candidates).

Athena on-demand pricing is per TB scanned, so recurring heavy queries over large,
unoptimized tables are the main cost driver. This report groups the recent query
history of a workgroup by normalized SQL text and ranks it by total GB scanned.

    python athena/athena_scan_report.py --workgroup primary --days 30 --top 20
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import boto3

PRICE_PER_TB_USD = 5.0  # on-demand price in most regions; adjust to your region/contract


def normalize(sql: str) -> str:
    sql = re.sub(r"'[^']*'", "'?'", sql)          # literals
    sql = re.sub(r"\b\d+\b", "?", sql)            # numbers
    return re.sub(r"\s+", " ", sql).strip()[:200]


def fetch_executions(client, workgroup: str, since: datetime):
    paginator = client.get_paginator("list_query_executions")
    for page in paginator.paginate(WorkGroup=workgroup):
        ids = page["QueryExecutionIds"]
        if not ids:
            continue
        for start in range(0, len(ids), 50):
            batch = client.batch_get_query_execution(QueryExecutionIds=ids[start:start + 50])
            for execution in batch["QueryExecutions"]:
                submitted = execution["Status"]["SubmissionDateTime"]
                if submitted < since:
                    return
                yield execution


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workgroup", default="primary")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    since = datetime.now(UTC) - timedelta(days=args.days)
    stats: dict[str, dict] = defaultdict(lambda: {"runs": 0, "bytes": 0})
    for execution in fetch_executions(boto3.client("athena"), args.workgroup, since):
        key = normalize(execution["Query"])
        stats[key]["runs"] += 1
        stats[key]["bytes"] += execution.get("Statistics", {}).get("DataScannedInBytes", 0)

    ranking = sorted(stats.items(), key=lambda item: item[1]["bytes"], reverse=True)[: args.top]
    print(f"{'runs':>6} {'GB scanned':>12} {'est. USD':>10}  query")
    for query, value in ranking:
        gb = value["bytes"] / 1024**3
        print(f"{value['runs']:>6} {gb:>12.1f} {gb / 1024 * PRICE_PER_TB_USD:>10.2f}  {query}")


if __name__ == "__main__":
    main()
