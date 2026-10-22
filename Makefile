# Local pipeline (no cloud account needed): sample raw files -> bronze -> dbt -> validation
PY      ?= python
export BRONZE_PATH := $(CURDIR)/data/bronze
export DUCKDB_PATH := $(CURDIR)/data/warehouse.duckdb
export DBT_SEND_ANONYMOUS_USAGE_STATS := false

.PHONY: setup lint test ingest bronze dbt validate all package-glue airflow-setup airflow-test clean

setup:
	$(PY) -m pip install -r requirements.txt

lint:
	ruff check .

test:
	$(PY) -m pytest -q

ingest:
	$(PY) ingestion/ingest_despesas.py --input-dir sample_data/raw --target data/raw --clean

bronze:
	$(PY) local/build_bronze.py --raw data/raw --bronze data/bronze

dbt:
	cd dbt && dbt build --profiles-dir . --target local

validate:
	$(PY) validation/shadow_validation.py --legacy local --new local

all: lint test ingest bronze dbt validate

# Package the shared schema module used by the Glue job (--extra-py-files)
package-glue:
	$(PY) -c "import shutil; shutil.make_archive('data/common', 'zip', '.', 'common')"
	@echo "upload: aws s3 cp data/common.zip s3://<bucket>/code/glue/common.zip"

# ------------------------------------------------------------------------------------
# Airflow: run the full monthly pipeline once, end to end, without a server
# (same orchestration as production, local mode with the sample files)
# ------------------------------------------------------------------------------------
AIRFLOW_VERSION := 3.1.8
AIRFLOW_PY      := $(shell $(PY) -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
export AIRFLOW_HOME := $(CURDIR)/data/airflow
export AIRFLOW__CORE__DAGS_FOLDER := $(CURDIR)/orchestration/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES := False
export PSDP_TARGET ?= local

# dbt gets its own environment (.dbt-venv) so its libraries never conflict with Airflow's
airflow-setup:
	$(PY) -m venv .dbt-venv
	.dbt-venv/bin/pip install -q -r orchestration/airflow/requirements-dbt.txt
	$(PY) -m pip install "apache-airflow==$(AIRFLOW_VERSION)" -r orchestration/airflow/requirements-airflow.txt pytest \
		--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(AIRFLOW_PY).txt"

airflow-test: export DBT_BIN := $(CURDIR)/.dbt-venv/bin/dbt
airflow-test:
	airflow db migrate
	airflow dags test public_spending_monthly

clean:
	rm -rf data dbt/target dbt/logs validation/reports
