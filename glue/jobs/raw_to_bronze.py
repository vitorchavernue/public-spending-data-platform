"""AWS Glue job (PySpark): raw Despesas CSV -> bronze Parquet.

Reads the raw monthly files exactly as published (latin-1, ';', decimal ','),
standardizes column names, casts amounts to DECIMAL(18,2), adds lineage columns
and writes Parquet partitioned by ``ano_mes`` with dynamic partition overwrite,
so reprocessing one month never touches the others.

Job parameters
--------------
--RAW_PATH      s3://<bucket>/raw/despesas/
--BRONZE_PATH   s3://<bucket>/bronze/despesas/
--ANO_MES       optional YYYYMM to process a single month (default: all)

The shared schema module is shipped with ``--extra-py-files common.zip``.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.schema import AMOUNT_COLUMNS, COLUMN_MAP, RAW_ENCODING, RAW_SEPARATOR


def transform(df: DataFrame) -> DataFrame:
    for raw_name, bronze_name in COLUMN_MAP.items():
        df = df.withColumnRenamed(raw_name, bronze_name)

    for column in df.columns:
        df = df.withColumn(column, F.trim(F.col(column)))

    for column in AMOUNT_COLUMNS:
        df = df.withColumn(
            column,
            F.regexp_replace(F.col(column), ",", ".").cast("decimal(18,2)"),
        )

    return (
        df.withColumn("ano_mes", F.regexp_replace(F.col("ano_mes"), "/", ""))
        .withColumn("_source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def main() -> None:
    optional = ["ANO_MES"] if "--ANO_MES" in sys.argv else []
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "BRONZE_PATH", *optional])

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    source = args["RAW_PATH"].rstrip("/")
    if args.get("ANO_MES"):
        source = f"{source}/ano_mes={args['ANO_MES']}"

    raw = (
        spark.read.option("header", "true")
        .option("sep", RAW_SEPARATOR)
        .option("encoding", RAW_ENCODING)
        .option("quote", '"')
        .option("recursiveFileLookup", "true")
        .csv(source)
    )

    bronze = transform(raw)
    (
        bronze.repartition("ano_mes")
        .write.mode("overwrite")
        .partitionBy("ano_mes")
        .option("compression", "snappy")
        .parquet(args["BRONZE_PATH"])
    )

    job.commit()


if __name__ == "__main__":
    main()
