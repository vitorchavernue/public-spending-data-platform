-- Monthly budget execution per superior body: the main table consumed by Power BI
-- and the reference set used by the shadow validation (legacy vs Snowflake).
select
    f.ano_mes,
    o.cod_orgao_superior,
    o.nome_orgao_superior,
    count(*)                                                    as qtd_lancamentos,
    sum(f.valor_empenhado)                                      as valor_empenhado,
    sum(f.valor_liquidado)                                      as valor_liquidado,
    sum(f.valor_pago)                                           as valor_pago,
    sum(f.valor_rap_pago)                                       as valor_rap_pago,
    case when sum(f.valor_empenhado) > 0
         then round(sum(f.valor_pago) / sum(f.valor_empenhado), 4)
    end                                                         as taxa_execucao
from {{ ref('fct_despesas') }} as f
inner join {{ ref('dim_orgao') }} as o
    on f.orgao_key = o.orgao_key
group by 1, 2, 3
