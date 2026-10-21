{# Cross-database helpers: the same models run on DuckDB (local/CI) and Snowflake. #}

{% macro surrogate_key(columns) -%}
    md5(
        {%- for column in columns %}
        coalesce(cast({{ column }} as varchar), '_null_'){% if not loop.last %} || '|' ||{% endif %}
        {%- endfor %}
    )
{%- endmacro %}

{% macro month_start(ano_mes) -%}
    {%- if target.type == 'duckdb' -%}
        cast(strptime({{ ano_mes }} || '01', '%Y%m%d') as date)
    {%- else -%}
        to_date({{ ano_mes }} || '01', 'YYYYMMDD')
    {%- endif -%}
{%- endmacro %}

{% macro clean_text(column) -%}
    nullif(trim({{ column }}), '')
{%- endmacro %}
