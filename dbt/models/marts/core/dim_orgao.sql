-- One row per subordinate agency, with its superior body (latest name wins).
select
    {{ surrogate_key(['cod_orgao_subordinado']) }}  as orgao_key,
    cod_orgao_subordinado,
    nome_orgao_subordinado,
    cod_orgao_superior,
    nome_orgao_superior
from {{ ref('stg_despesas') }}
qualify row_number() over (partition by cod_orgao_subordinado order by ano_mes desc) = 1
