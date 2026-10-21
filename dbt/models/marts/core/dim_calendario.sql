-- Monthly calendar for the months present in the data.
with meses as (
    select distinct ano_mes, mes_referencia from {{ ref('stg_despesas') }}
)

select
    ano_mes,
    mes_referencia,
    extract(year from mes_referencia)              as ano,
    extract(month from mes_referencia)             as mes,
    extract(quarter from mes_referencia)           as trimestre,
    case when extract(month from mes_referencia) <= 6 then 1 else 2 end as semestre
from meses
