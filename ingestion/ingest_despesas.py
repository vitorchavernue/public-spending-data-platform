"""Land monthly Despesas files into the raw zone (S3 or local folder).

Raw files are stored unchanged (original encoding and layout) and partitioned
by month, so the raw zone is an exact, auditable copy of the source:

    <target>/despesas/ano_mes=YYYYMM/YYYYMM_Despesas.csv

Examples
--------
Download two months from the Portal da Transparência and land them in S3:

    python ingestion/ingest_despesas.py --months 202603 202604 \
        --target s3://my-data-lake/raw

Land files that were already downloaded manually into a local folder:

    python ingestion/ingest_despesas.py --input-dir ./downloads --target ./data/raw
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import re
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path

# Template of the monthly "Execução da Despesa" download. Configurable through the
# DESPESAS_URL_TEMPLATE environment variable in case the portal changes its URL.
DEFAULT_URL_TEMPLATE = (
    "https://portaldatransparencia.gov.br/download-de-dados/despesas-execucao/{ano_mes}"
)
FILE_PATTERN = re.compile(r"(?P<ano_mes>\d{6})_Despesas\.csv$", re.IGNORECASE)

log = logging.getLogger("ingest_despesas")


def partition_key(target: str, ano_mes: str, file_name: str) -> str:
    return f"{target.rstrip('/')}/despesas/ano_mes={ano_mes}/{file_name}"


def write_raw(target: str, ano_mes: str, file_name: str, content: bytes) -> str:
    """Write the raw file to S3 (s3://bucket/prefix) or to a local folder."""
    dest = partition_key(target, ano_mes, file_name)
    if dest.startswith("s3://"):
        import boto3  # imported lazily so local runs do not require AWS libs

        bucket, key = dest[5:].split("/", 1)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=content)
    else:
        path = Path(dest)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    log.info("landed %s (%.1f MB)", dest, len(content) / 1_048_576)
    return dest


def extract_csvs(payload: bytes) -> dict[str, bytes]:
    """Return {file_name: content} for every Despesas CSV inside a zip payload."""
    files: dict[str, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for name in archive.namelist():
            if FILE_PATTERN.search(Path(name).name):
                files[Path(name).name] = archive.read(name)
    return files


def download_month(ano_mes: str, url_template: str) -> dict[str, bytes]:
    url = url_template.format(ano_mes=ano_mes)
    log.info("downloading %s", url)
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=300) as response:
        return extract_csvs(response.read())


def collect_local(input_dir: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for path in sorted(input_dir.iterdir()):
        if path.suffix.lower() == ".zip":
            files.update(extract_csvs(path.read_bytes()))
        elif FILE_PATTERN.search(path.name):
            files[path.name] = path.read_bytes()
    return files


def run(months: list[str], input_dir: Path | None, target: str, url_template: str) -> list[str]:
    files: dict[str, bytes] = {}
    if input_dir:
        files.update(collect_local(input_dir))
    for ano_mes in months:
        files.update(download_month(ano_mes, url_template))
    if not files:
        raise SystemExit("no Despesas files found to ingest")

    landed = []
    for file_name, content in sorted(files.items()):
        ano_mes = FILE_PATTERN.search(file_name).group("ano_mes")
        landed.append(write_raw(target, ano_mes, file_name, content))
    return landed


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--months", nargs="*", default=[], help="months to download (YYYYMM)")
    parser.add_argument("--input-dir", type=Path, help="folder with already downloaded .zip/.csv files")
    parser.add_argument("--target", required=True, help="s3://bucket/prefix or local folder")
    parser.add_argument("--clean", action="store_true", help="remove the local target folder first")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.clean and not args.target.startswith("s3://"):
        shutil.rmtree(Path(args.target) / "despesas", ignore_errors=True)

    url_template = os.environ.get("DESPESAS_URL_TEMPLATE", DEFAULT_URL_TEMPLATE)
    run(args.months, args.input_dir, args.target, url_template)


if __name__ == "__main__":
    sys.exit(main())
