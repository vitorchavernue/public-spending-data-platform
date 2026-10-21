# Migration runbook — Athena → Snowflake

1. **Inventory and prioritization**
   - Rank Athena queries by GB scanned with `athena/athena_scan_report.py`.
   - Select the recurring, high-scan workloads whose estimated Snowflake cost is lower
     (see [cost_analysis.md](cost_analysis.md)). Independent workloads go first.

2. **Load the same bronze layer into Snowflake**
   - Storage integration + external stage on `s3://<bucket>/bronze/` (`snowflake/02_load_bronze.sql`).
   - Idempotent `COPY INTO` scheduled by a Snowflake task right after the Glue job.

3. **Rebuild the business logic with dbt**
   - Silver (`stg_despesas`) and gold (star schema + `agg_execucao_mensal`).
   - Data tests: keys, relationships, accepted values, reconciliation silver ↔ gold.

4. **Shadow mode (parallel run)**
   - Both platforms run daily. `validation/shadow_validation.py --legacy athena --new snowflake`
     compares key coverage and metric adherence per month and superior body.
   - Cut-over criterion: **≥ 99.5% adherence on every critical metric** for consecutive runs.
   - Divergences are investigated with the report (missing keys, largest differences): typical
     causes are filters, null handling, casting of decimals, duplicated loads and late files.

5. **Homologation with the business area**
   - Power BI reports pointed to Snowflake in a staging workspace, validated by the business users.

6. **Cut-over and monitoring**
   - Power BI datasets switched to `ANALYTICS.GOLD` using the `REPORTER` role and `WH_BI`.
   - Athena workload retired; credits and query performance monitored with
     `snowflake/03_cost_monitoring.sql`.
