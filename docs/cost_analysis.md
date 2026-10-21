# Cost analysis — why part of the workload moves from Athena to Snowflake

The two engines are billed in different ways, and that difference is what drives the migration
decision:

| | Athena (on-demand) | Snowflake |
|---|---|---|
| Unit of cost | data **scanned** per query (USD per TB, ~5 USD in most regions) | **time** a warehouse is running (credits per hour, billed per second, 60 s minimum) |
| Cost grows with | size of the tables read × number of executions | warehouse size × active time |
| Cheap when | few, well-filtered queries on partitioned/columnar data | many repeated queries over the same data, with auto-suspend |
| Expensive when | wide `SELECT`s on large tables without partition filters, run many times a day | warehouses left running or oversized |

## Decision rule

```
Athena cost / month    = Σ (TB scanned per run × runs per month) × price per TB
Snowflake cost / month = Σ (active hours per month × credits per hour of the warehouse) × price per credit
```

Workloads are migrated **only when the Snowflake estimate is clearly lower**. Ad-hoc,
infrequent and well-partitioned queries stay on Athena, where they cost almost nothing.

## Illustrative scenario

> Example numbers to show the method — replace them with the values from
> `athena/athena_scan_report.py` and `snowflake/03_cost_monitoring.sql`.

A BI team runs recurring heavy queries over a large table without an effective partition filter:

| Item | Value |
|---|---|
| Heavy queries per business day | 40 |
| Data scanned per query | 150 GB |
| Business days per month | 22 |
| **Athena** — 40 × 0.15 TB × 22 × 5 USD | **≈ 660 USD / month** |

After the migration the same queries run on a clustered Snowflake table (pruning by `ano_mes`),
on an X-Small warehouse (1 credit/hour) with a 60-second auto-suspend:

| Item | Value |
|---|---|
| Effective active time of WH_BI per business day | ~3 h |
| Credits per month — 3 h × 1 credit × 22 | 66 |
| **Snowflake** — 66 credits × 3 USD | **≈ 200 USD / month** |

## Guardrails implemented in this project

- **Athena workgroup scan cutoff** (`infra/terraform/athena.tf`): any query that would scan more
  than the configured limit is cancelled.
- **Partition projection** on `ano_mes` (`athena/01_bronze_despesas.sql`): queries filtered by month
  only read that month.
- **One small warehouse per workload** with 60-second auto-suspend (`snowflake/01_setup.sql`).
- **Resource monitors** that notify at 80% and suspend at 100% of the monthly credit quota.
- **Monitoring queries** for credits per warehouse, spilling (warehouse too small) and queueing
  (`snowflake/03_cost_monitoring.sql`).
