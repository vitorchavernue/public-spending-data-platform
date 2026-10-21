-- Gold fact: expenditure execution per line, linked to the dimensions by surrogate keys.
select
    s.despesa_id,
    s.ano_mes,
    {{ surrogate_key(['s.cod_orgao_subordinado']) }}                                   as orgao_key,
    {{ surrogate_key(['s.cod_funcao', 's.cod_subfuncao']) }}                          as funcao_key,
    {{ surrogate_key(['s.cod_programa', 's.cod_acao']) }}                             as programa_key,
    {{ surrogate_key(['s.cod_categoria_economica', 's.cod_grupo_despesa', 's.cod_elemento_despesa', 's.cod_modalidade_despesa']) }}
                                                                                      as natureza_key,
    {{ surrogate_key(['s.uf']) }}                                                     as localidade_key,
    s.cod_unidade_gestora,
    s.nome_unidade_gestora,
    s.municipio,
    s.cod_autor_emenda <> '-1'                                                        as possui_emenda,
    s.valor_empenhado,
    s.valor_liquidado,
    s.valor_pago,
    s.valor_rap_inscrito,
    s.valor_rap_cancelado,
    s.valor_rap_pago
from {{ ref('stg_despesas') }} as s
