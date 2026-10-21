"""Single source of truth for the Despesas (public expenditure) dataset schema.

The raw files published by the Portal da Transparência use Portuguese column
names with accents and spaces, ``;`` as separator, ``latin-1`` encoding and
``,`` as decimal separator. Every layer (Glue job, local builder, validation)
imports this mapping so the bronze schema is identical everywhere.
"""

from __future__ import annotations

# raw column name -> bronze column name
COLUMN_MAP: dict[str, str] = {
    "Ano e mês do lançamento": "ano_mes",
    "Código Órgão Superior": "cod_orgao_superior",
    "Nome Órgão Superior": "nome_orgao_superior",
    "Código Órgão Subordinado": "cod_orgao_subordinado",
    "Nome Órgão Subordinado": "nome_orgao_subordinado",
    "Código Unidade Gestora": "cod_unidade_gestora",
    "Nome Unidade Gestora": "nome_unidade_gestora",
    "Código Gestão": "cod_gestao",
    "Nome Gestão": "nome_gestao",
    "Código Unidade Orçamentária": "cod_unidade_orcamentaria",
    "Nome Unidade Orçamentária": "nome_unidade_orcamentaria",
    "Código Função": "cod_funcao",
    "Nome Função": "nome_funcao",
    "Código Subfução": "cod_subfuncao",  # typo kept from the official source
    "Nome Subfunção": "nome_subfuncao",
    "Código Programa Orçamentário": "cod_programa",
    "Nome Programa Orçamentário": "nome_programa",
    "Código Ação": "cod_acao",
    "Nome Ação": "nome_acao",
    "Código Plano Orçamentário": "cod_plano_orcamentario",
    "Plano Orçamentário": "nome_plano_orcamentario",
    "Código Programa Governo": "cod_programa_governo",
    "Nome Programa Governo": "nome_programa_governo",
    "UF": "uf",
    "Município": "municipio",
    "Código Subtítulo": "cod_subtitulo",
    "Nome Subtítulo": "nome_subtitulo",
    "Código Localizador": "cod_localizador",
    "Nome Localizador": "nome_localizador",
    "Sigla Localizador": "sigla_localizador",
    "Descrição Complementar Localizador": "desc_complementar_localizador",
    "Código Autor Emenda": "cod_autor_emenda",
    "Nome Autor Emenda": "nome_autor_emenda",
    "Código Categoria Econômica": "cod_categoria_economica",
    "Nome Categoria Econômica": "nome_categoria_economica",
    "Código Grupo de Despesa": "cod_grupo_despesa",
    "Nome Grupo de Despesa": "nome_grupo_despesa",
    "Código Elemento de Despesa": "cod_elemento_despesa",
    "Nome Elemento de Despesa": "nome_elemento_despesa",
    "Código Modalidade da Despesa": "cod_modalidade_despesa",
    "Modalidade da Despesa": "nome_modalidade_despesa",
    "Valor Empenhado (R$)": "valor_empenhado",
    "Valor Liquidado (R$)": "valor_liquidado",
    "Valor Pago (R$)": "valor_pago",
    "Valor Restos a Pagar Inscritos (R$)": "valor_rap_inscrito",
    "Valor Restos a Pagar Cancelado (R$)": "valor_rap_cancelado",
    "Valor Restos a Pagar Pagos (R$)": "valor_rap_pago",
}

AMOUNT_COLUMNS: list[str] = [
    "valor_empenhado",
    "valor_liquidado",
    "valor_pago",
    "valor_rap_inscrito",
    "valor_rap_cancelado",
    "valor_rap_pago",
]

RAW_ENCODING = "latin-1"
RAW_SEPARATOR = ";"

BRONZE_COLUMNS: list[str] = list(COLUMN_MAP.values())
