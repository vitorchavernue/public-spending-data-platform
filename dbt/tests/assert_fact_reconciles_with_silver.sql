-- The gold fact must keep every row and every cent of the silver layer.
with silver as (
    select count(*) as linhas, sum(valor_empenhado) as empenhado, sum(valor_pago) as pago
    from {{ ref('stg_despesas') }}
),
gold as (
    select count(*) as linhas, sum(valor_empenhado) as empenhado, sum(valor_pago) as pago
    from {{ ref('fct_despesas') }}
)
select *
from silver
cross join gold as g
where silver.linhas <> g.linhas
   or silver.empenhado <> g.empenhado
   or silver.pago <> g.pago
