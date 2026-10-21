-- States (UF) with their region. 'NI' = not informed (national-level spending).
with ufs as (
    select distinct uf from {{ ref('stg_despesas') }}
)

select
    {{ surrogate_key(['uf']) }} as localidade_key,
    uf,
    case
        when uf in ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') then 'Norte'
        when uf in ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') then 'Nordeste'
        when uf in ('DF', 'GO', 'MS', 'MT') then 'Centro-Oeste'
        when uf in ('ES', 'MG', 'RJ', 'SP') then 'Sudeste'
        when uf in ('PR', 'RS', 'SC') then 'Sul'
        when uf = 'EX' then 'Exterior'
        else 'Não informado'
    end as regiao
from ufs
