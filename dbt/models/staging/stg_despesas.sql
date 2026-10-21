{{ config(materialized='view') }}

-- Silver: typed, cleaned and standardized expenditure lines (one row per source line).

with source as (

    select * from {{ source('bronze', 'despesas') }}

),

cleaned as (

    select
        cast(ano_mes as varchar)                                    as ano_mes,
        {{ month_start("cast(ano_mes as varchar)") }}               as mes_referencia,

        {{ clean_text('cod_orgao_superior') }}                      as cod_orgao_superior,
        {{ clean_text('nome_orgao_superior') }}                     as nome_orgao_superior,
        {{ clean_text('cod_orgao_subordinado') }}                   as cod_orgao_subordinado,
        {{ clean_text('nome_orgao_subordinado') }}                  as nome_orgao_subordinado,
        {{ clean_text('cod_unidade_gestora') }}                     as cod_unidade_gestora,
        {{ clean_text('nome_unidade_gestora') }}                    as nome_unidade_gestora,
        {{ clean_text('cod_gestao') }}                              as cod_gestao,
        {{ clean_text('cod_unidade_orcamentaria') }}                as cod_unidade_orcamentaria,

        {{ clean_text('cod_funcao') }}                              as cod_funcao,
        {{ clean_text('nome_funcao') }}                             as nome_funcao,
        {{ clean_text('cod_subfuncao') }}                           as cod_subfuncao,
        {{ clean_text('nome_subfuncao') }}                          as nome_subfuncao,
        {{ clean_text('cod_programa') }}                            as cod_programa,
        {{ clean_text('nome_programa') }}                           as nome_programa,
        {{ clean_text('cod_acao') }}                                as cod_acao,
        {{ clean_text('nome_acao') }}                               as nome_acao,
        {{ clean_text('cod_plano_orcamentario') }}                  as cod_plano_orcamentario,
        {{ clean_text('cod_programa_governo') }}                    as cod_programa_governo,

        coalesce(upper({{ clean_text('uf') }}), 'NI')               as uf,
        {{ clean_text('municipio') }}                               as municipio,
        {{ clean_text('cod_subtitulo') }}                           as cod_subtitulo,
        {{ clean_text('cod_localizador') }}                         as cod_localizador,
        {{ clean_text('cod_autor_emenda') }}                        as cod_autor_emenda,
        {{ clean_text('nome_autor_emenda') }}                       as nome_autor_emenda,

        {{ clean_text('cod_categoria_economica') }}                 as cod_categoria_economica,
        {{ clean_text('nome_categoria_economica') }}                as nome_categoria_economica,
        {{ clean_text('cod_grupo_despesa') }}                       as cod_grupo_despesa,
        {{ clean_text('nome_grupo_despesa') }}                      as nome_grupo_despesa,
        {{ clean_text('cod_elemento_despesa') }}                    as cod_elemento_despesa,
        {{ clean_text('nome_elemento_despesa') }}                   as nome_elemento_despesa,
        {{ clean_text('cod_modalidade_despesa') }}                  as cod_modalidade_despesa,
        {{ clean_text('nome_modalidade_despesa') }}                 as nome_modalidade_despesa,

        coalesce(valor_empenhado, 0)                                as valor_empenhado,
        coalesce(valor_liquidado, 0)                                as valor_liquidado,
        coalesce(valor_pago, 0)                                     as valor_pago,
        coalesce(valor_rap_inscrito, 0)                             as valor_rap_inscrito,
        coalesce(valor_rap_cancelado, 0)                            as valor_rap_cancelado,
        coalesce(valor_rap_pago, 0)                                 as valor_rap_pago,

        _source_file,
        _ingested_at

    from source

)

select
    {{ surrogate_key([
        'ano_mes', 'cod_unidade_gestora', 'cod_gestao', 'cod_unidade_orcamentaria',
        'cod_funcao', 'cod_subfuncao', 'cod_programa', 'cod_acao', 'cod_plano_orcamentario',
        'cod_programa_governo', 'uf', 'municipio', 'cod_subtitulo', 'cod_localizador',
        'cod_autor_emenda', 'cod_categoria_economica', 'cod_grupo_despesa',
        'cod_elemento_despesa', 'cod_modalidade_despesa'
    ]) }}                                                           as despesa_id,
    *
from cleaned
