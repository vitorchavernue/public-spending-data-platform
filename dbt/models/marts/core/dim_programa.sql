-- Budget program + action.
select
    {{ surrogate_key(['cod_programa', 'cod_acao']) }}  as programa_key,
    cod_programa,
    nome_programa,
    cod_acao,
    nome_acao
from {{ ref('stg_despesas') }}
qualify row_number() over (partition by cod_programa, cod_acao order by ano_mes desc) = 1
