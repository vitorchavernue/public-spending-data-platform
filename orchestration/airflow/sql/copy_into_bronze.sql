-- =====================================================================================
-- Load the standardized monthly files (bronze layer in S3) into Snowflake.
--
-- What it does, in plain words:
--   * Reads only the months being processed in this run (the list comes from Airflow).
--   * Copies each expenditure line into the table RAW.BRONZE.DESPESAS.
--   * Is safe to run again: Snowflake remembers which files were already loaded and
--     skips them, so a retry never duplicates money in the reports.
--   * Stops at the first broken file (ON_ERROR = ABORT_STATEMENT), because loading
--     a month partially would show wrong totals to the business.
-- =====================================================================================

USE ROLE LOADER;
USE WAREHOUSE WH_LOAD;   -- small warehouse that switches itself off after 60 seconds idle

COPY INTO RAW.BRONZE.DESPESAS (
    ano_mes,
    cod_orgao_superior,
    nome_orgao_superior,
    cod_orgao_subordinado,
    nome_orgao_subordinado,
    cod_unidade_gestora,
    nome_unidade_gestora,
    cod_gestao,
    nome_gestao,
    cod_unidade_orcamentaria,
    nome_unidade_orcamentaria,
    cod_funcao,
    nome_funcao,
    cod_subfuncao,
    nome_subfuncao,
    cod_programa,
    nome_programa,
    cod_acao,
    nome_acao,
    cod_plano_orcamentario,
    nome_plano_orcamentario,
    cod_programa_governo,
    nome_programa_governo,
    uf,
    municipio,
    cod_subtitulo,
    nome_subtitulo,
    cod_localizador,
    nome_localizador,
    sigla_localizador,
    desc_complementar_localizador,
    cod_autor_emenda,
    nome_autor_emenda,
    cod_categoria_economica,
    nome_categoria_economica,
    cod_grupo_despesa,
    nome_grupo_despesa,
    cod_elemento_despesa,
    nome_elemento_despesa,
    cod_modalidade_despesa,
    nome_modalidade_despesa,
    valor_empenhado,
    valor_liquidado,
    valor_pago,
    valor_rap_inscrito,
    valor_rap_cancelado,
    valor_rap_pago,
    _source_file,
    _ingested_at
)
FROM (
    SELECT
        -- The month is part of the folder name (ano_mes=YYYYMM), not of the file content.
        REGEXP_SUBSTR(METADATA$FILENAME, 'ano_mes=([0-9]{6})', 1, 1, 'e', 1),
        $1:cod_orgao_superior::VARCHAR,
        $1:nome_orgao_superior::VARCHAR,
        $1:cod_orgao_subordinado::VARCHAR,
        $1:nome_orgao_subordinado::VARCHAR,
        $1:cod_unidade_gestora::VARCHAR,
        $1:nome_unidade_gestora::VARCHAR,
        $1:cod_gestao::VARCHAR,
        $1:nome_gestao::VARCHAR,
        $1:cod_unidade_orcamentaria::VARCHAR,
        $1:nome_unidade_orcamentaria::VARCHAR,
        $1:cod_funcao::VARCHAR,
        $1:nome_funcao::VARCHAR,
        $1:cod_subfuncao::VARCHAR,
        $1:nome_subfuncao::VARCHAR,
        $1:cod_programa::VARCHAR,
        $1:nome_programa::VARCHAR,
        $1:cod_acao::VARCHAR,
        $1:nome_acao::VARCHAR,
        $1:cod_plano_orcamentario::VARCHAR,
        $1:nome_plano_orcamentario::VARCHAR,
        $1:cod_programa_governo::VARCHAR,
        $1:nome_programa_governo::VARCHAR,
        $1:uf::VARCHAR,
        $1:municipio::VARCHAR,
        $1:cod_subtitulo::VARCHAR,
        $1:nome_subtitulo::VARCHAR,
        $1:cod_localizador::VARCHAR,
        $1:nome_localizador::VARCHAR,
        $1:sigla_localizador::VARCHAR,
        $1:desc_complementar_localizador::VARCHAR,
        $1:cod_autor_emenda::VARCHAR,
        $1:nome_autor_emenda::VARCHAR,
        $1:cod_categoria_economica::VARCHAR,
        $1:nome_categoria_economica::VARCHAR,
        $1:cod_grupo_despesa::VARCHAR,
        $1:nome_grupo_despesa::VARCHAR,
        $1:cod_elemento_despesa::VARCHAR,
        $1:nome_elemento_despesa::VARCHAR,
        $1:cod_modalidade_despesa::VARCHAR,
        $1:nome_modalidade_despesa::VARCHAR,
        $1:valor_empenhado::NUMBER(18,2),
        $1:valor_liquidado::NUMBER(18,2),
        $1:valor_pago::NUMBER(18,2),
        $1:valor_rap_inscrito::NUMBER(18,2),
        $1:valor_rap_cancelado::NUMBER(18,2),
        $1:valor_rap_pago::NUMBER(18,2),
        $1:_source_file::VARCHAR,
        $1:_ingested_at::TIMESTAMP_NTZ
    FROM @RAW.BRONZE.STG_BRONZE_DESPESAS
)
-- Only the folders of the months requested by this pipeline run.
PATTERN = '.*ano_mes=({{ ti.xcom_pull(task_ids="define_months") | join("|") }})/.*[.]parquet'
ON_ERROR = ABORT_STATEMENT;
