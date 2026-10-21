-- Athena (legacy analytics layer): bronze table over the Parquet files written by Glue.
-- Partition projection avoids MSCK REPAIR / crawlers: new months are queryable as soon
-- as the Glue job writes them.

CREATE EXTERNAL TABLE IF NOT EXISTS bronze.despesas (
    cod_orgao_superior            string,
    nome_orgao_superior           string,
    cod_orgao_subordinado         string,
    nome_orgao_subordinado        string,
    cod_unidade_gestora           string,
    nome_unidade_gestora          string,
    cod_gestao                    string,
    nome_gestao                   string,
    cod_unidade_orcamentaria      string,
    nome_unidade_orcamentaria     string,
    cod_funcao                    string,
    nome_funcao                   string,
    cod_subfuncao                 string,
    nome_subfuncao                string,
    cod_programa                  string,
    nome_programa                 string,
    cod_acao                      string,
    nome_acao                     string,
    cod_plano_orcamentario        string,
    nome_plano_orcamentario       string,
    cod_programa_governo          string,
    nome_programa_governo         string,
    uf                            string,
    municipio                     string,
    cod_subtitulo                 string,
    nome_subtitulo                string,
    cod_localizador               string,
    nome_localizador              string,
    sigla_localizador             string,
    desc_complementar_localizador string,
    cod_autor_emenda              string,
    nome_autor_emenda             string,
    cod_categoria_economica       string,
    nome_categoria_economica      string,
    cod_grupo_despesa             string,
    nome_grupo_despesa            string,
    cod_elemento_despesa          string,
    nome_elemento_despesa         string,
    cod_modalidade_despesa        string,
    nome_modalidade_despesa       string,
    valor_empenhado               decimal(18,2),
    valor_liquidado               decimal(18,2),
    valor_pago                    decimal(18,2),
    valor_rap_inscrito            decimal(18,2),
    valor_rap_cancelado           decimal(18,2),
    valor_rap_pago                decimal(18,2),
    _source_file                  string,
    _ingested_at                  timestamp
)
PARTITIONED BY (ano_mes string)
STORED AS PARQUET
LOCATION 's3://${DATA_LAKE_BUCKET}/bronze/despesas/'
TBLPROPERTIES (
    'parquet.compression'          = 'SNAPPY',
    'projection.enabled'           = 'true',
    'projection.ano_mes.type'      = 'date',
    'projection.ano_mes.format'    = 'yyyyMM',
    'projection.ano_mes.range'     = '202001,NOW',
    'projection.ano_mes.interval'  = '1',
    'projection.ano_mes.interval.unit' = 'MONTHS',
    'storage.location.template'    = 's3://${DATA_LAKE_BUCKET}/bronze/despesas/ano_mes=${ano_mes}/'
);
