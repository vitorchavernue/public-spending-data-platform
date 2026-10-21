-- Nature of expense: economic category, expense group, element and modality.
select
    {{ surrogate_key(['cod_categoria_economica', 'cod_grupo_despesa', 'cod_elemento_despesa', 'cod_modalidade_despesa']) }}
                                as natureza_key,
    cod_categoria_economica,
    nome_categoria_economica,
    cod_grupo_despesa,
    nome_grupo_despesa,
    cod_elemento_despesa,
    nome_elemento_despesa,
    cod_modalidade_despesa,
    nome_modalidade_despesa
from {{ ref('stg_despesas') }}
qualify row_number() over (
    partition by cod_categoria_economica, cod_grupo_despesa, cod_elemento_despesa, cod_modalidade_despesa
    order by ano_mes desc
) = 1
