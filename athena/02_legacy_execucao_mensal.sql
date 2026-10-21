-- Legacy metric definition used by BI before the migration. It is also the "legacy side"
-- of the shadow validation against the Snowflake table gold.agg_execucao_mensal.
-- Always filter by ano_mes: it prunes partitions and limits the bytes scanned (cost).

CREATE OR REPLACE VIEW analytics.legacy_execucao_mensal AS
SELECT
    ano_mes,
    trim(cod_orgao_superior)              AS cod_orgao_superior,
    count(*)                              AS qtd_lancamentos,
    sum(coalesce(valor_empenhado, 0))     AS valor_empenhado,
    sum(coalesce(valor_liquidado, 0))     AS valor_liquidado,
    sum(coalesce(valor_pago, 0))          AS valor_pago
FROM bronze.despesas
GROUP BY 1, 2;
