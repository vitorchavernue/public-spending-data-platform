-- Functional classification: function + subfunction.
select
    {{ surrogate_key(['cod_funcao', 'cod_subfuncao']) }}  as funcao_key,
    cod_funcao,
    nome_funcao,
    cod_subfuncao,
    nome_subfuncao
from {{ ref('stg_despesas') }}
qualify row_number() over (partition by cod_funcao, cod_subfuncao order by ano_mes desc) = 1
