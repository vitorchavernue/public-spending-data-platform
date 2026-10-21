import io
import zipfile

from ingestion.ingest_despesas import extract_csvs, partition_key, run


def make_zip(files):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def test_partition_key_is_hive_style():
    assert (
        partition_key("s3://lake/raw/", "202604", "202604_Despesas.csv")
        == "s3://lake/raw/despesas/ano_mes=202604/202604_Despesas.csv"
    )


def test_extract_only_despesas_csvs_from_zip():
    payload = make_zip({"202604_Despesas.csv": b"a;b", "readme.txt": b"x"})
    assert list(extract_csvs(payload)) == ["202604_Despesas.csv"]


def test_run_lands_local_files_unchanged(tmp_path):
    source = tmp_path / "downloads"
    source.mkdir()
    content = "Código;Valor\n1;10,50\n".encode("latin-1")
    (source / "202603_Despesas.csv").write_bytes(content)

    landed = run([], source, str(tmp_path / "raw"), url_template="unused")

    target = tmp_path / "raw" / "despesas" / "ano_mes=202603" / "202603_Despesas.csv"
    assert landed == [str(target)]
    assert target.read_bytes() == content
