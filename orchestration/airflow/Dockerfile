# Airflow image used to run the public spending pipeline on a laptop or a server.
# It is the official Apache Airflow image plus the libraries the pipeline needs.
FROM apache/airflow:3.1.8-python3.12

COPY requirements-airflow.txt requirements-dbt.txt /tmp/

# 1. Airflow add-ons, installed with the official "constraints" file, which pins
#    versions that are known to work together and avoids surprises after updates.
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt"

# 2. dbt in its own isolated environment (local DuckDB + Snowflake adapters),
#    so its libraries never conflict with Airflow's.
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# Tells the pipeline which dbt to use.
ENV DBT_BIN=/opt/airflow/dbt_venv/bin/dbt
