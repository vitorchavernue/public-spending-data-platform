# Local pipeline (no cloud account needed): sample raw files -> bronze -> dbt -> validation
PY      ?= python
export BRONZE_PATH := $(CURDIR)/data/bronze
export DUCKDB_PATH := $(CURDIR)/data/warehouse.duckdb
export DBT_SEND_ANONYMOUS_USAGE_STATS := false

.PHONY: setup lint test ingest bronze dbt validate all package-glue clean

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

clean:
	rm -rf data dbt/target dbt/logs validation/reports
