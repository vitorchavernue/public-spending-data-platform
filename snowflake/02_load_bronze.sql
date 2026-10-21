-- Load the bronze Parquet files (written by Glue in S3) into Snowflake.
-- This is the migration path: Snowflake reads the same bronze layer the legacy Athena
-- tables use, so both sides of the shadow validation start from identical data.

USE ROLE ACCOUNTADMIN;

-- S3 access through a storage integration (no keys stored in Snowflake) -------------------
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_DATA_LAKE
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<snowflake_s3_role_arn from terraform output>'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<data_lake_bucket>/bronze/');
-- DESC INTEGRATION S3_DATA_LAKE;  -> copy STORAGE_AWS_IAM_USER_ARN / EXTERNAL_ID to the IAM trust policy

GRANT USAGE ON INTEGRATION S3_DATA_LAKE TO ROLE LOADER;

USE ROLE LOADER;
USE WAREHOUSE WH_LOAD;
USE SCHEMA RAW.BRONZE;

CREATE FILE FORMAT IF NOT EXISTS FF_PARQUET TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS STG_BRONZE_DESPESAS
    URL = 's3://<data_lake_bucket>/bronze/despesas/'
    STORAGE_INTEGRATION = S3_DATA_LAKE
    FILE_FORMAT = FF_PARQUET;

CREATE TABLE IF NOT EXISTS DESPESAS (
    ano_mes                        VARCHAR(6) NOT NULL,
    cod_orgao_superior             VARCHAR,
    nome_orgao_superior            VARCHAR,
    cod_orgao_subordinado          VARCHAR,
    nome_orgao_subordinado         VARCHAR,
    cod_unidade_gestora            VARCHAR,
    nome_unidade_gestora           VARCHAR,
    cod_gestao                     VARCHAR,
    nome_gestao                    VARCHAR,
    cod_unidade_orcamentaria       VARCHAR,
    nome_unidade_orcamentaria      VARCHAR,
    cod_funcao                     VARCHAR,
    nome_funcao                    VARCHAR,
    cod_subfuncao                  VARCHAR,
    nome_subfuncao                 VARCHAR,
    cod_programa                   VARCHAR,
    nome_programa                  VARCHAR,
    cod_acao                       VARCHAR,
    nome_acao                      VARCHAR,
    cod_plano_orcamentario         VARCHAR,
    nome_plano_orcamentario        VARCHAR,
    cod_programa_governo           VARCHAR,
    nome_programa_governo          VARCHAR,
    uf                             VARCHAR,
    municipio                      VARCHAR,
    cod_subtitulo                  VARCHAR,
    nome_subtitulo                 VARCHAR,
    cod_localizador                VARCHAR,
    nome_localizador               VARCHAR,
    sigla_localizador              VARCHAR,
    desc_complementar_localizador  VARCHAR,
    cod_autor_emenda               VARCHAR,
    nome_autor_emenda              VARCHAR,
    cod_categoria_economica        VARCHAR,
    nome_categoria_economica       VARCHAR,
    cod_grupo_despesa              VARCHAR,
    nome_grupo_despesa             VARCHAR,
    cod_elemento_despesa           VARCHAR,
    nome_elemento_despesa          VARCHAR,
    cod_modalidade_despesa         VARCHAR,
    nome_modalidade_despesa        VARCHAR,
    valor_empenhado                NUMBER(18,2),
    valor_liquidado                NUMBER(18,2),
    valor_pago                     NUMBER(18,2),
    valor_rap_inscrito             NUMBER(18,2),
    valor_rap_cancelado            NUMBER(18,2),
    valor_rap_pago                 NUMBER(18,2),
    _source_file                   VARCHAR,
    _ingested_at                   TIMESTAMP_NTZ,
    _loaded_at                     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ano_mes)
COMMENT = 'Bronze expenditure lines loaded from S3 (Glue output)';

-- Idempotent load: Snowflake keeps 64 days of load metadata and skips files already loaded.
-- The partition value (ano_mes) only exists in the S3 path, so it is parsed from the file name.
COPY INTO DESPESAS (
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
    FROM @STG_BRONZE_DESPESAS
)
PATTERN = '.*ano_mes=[0-9]{6}/.*[.]parquet'
ON_ERROR = ABORT_STATEMENT;

-- Daily load right after the Glue job (05:00 America/Sao_Paulo) --------------------------
CREATE OR REPLACE TASK TSK_LOAD_BRONZE_DESPESAS
    WAREHOUSE = WH_LOAD
    SCHEDULE = 'USING CRON 0 5 * * * America/Sao_Paulo'
AS
    COPY INTO DESPESAS (
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
        FROM @STG_BRONZE_DESPESAS
    )
    PATTERN = '.*ano_mes=[0-9]{6}/.*[.]parquet'
    ON_ERROR = ABORT_STATEMENT;

ALTER TASK TSK_LOAD_BRONZE_DESPESAS RESUME;
