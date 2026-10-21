resource "aws_glue_catalog_database" "bronze" {
  name = "bronze"
}

resource "aws_glue_catalog_database" "analytics" {
  name = "analytics"
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "psdp-glue-job"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.data_lake.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data_lake.arn]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "data-lake-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "code/glue/raw_to_bronze.py"
  source = "${path.module}/../../glue/jobs/raw_to_bronze.py"
  etag   = filemd5("${path.module}/../../glue/jobs/raw_to_bronze.py")
}

resource "aws_glue_job" "raw_to_bronze" {
  name              = "psdp-raw-to-bronze"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_PATH"                         = "s3://${aws_s3_bucket.data_lake.id}/raw/despesas/"
    "--BRONZE_PATH"                      = "s3://${aws_s3_bucket.data_lake.id}/bronze/despesas/"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.data_lake.id}/code/glue/common.zip"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }
}

# Daily at 04:00 America/Sao_Paulo (07:00 UTC), before the Snowflake load task (05:00).
resource "aws_glue_trigger" "daily" {
  name     = "psdp-raw-to-bronze-daily"
  type     = "SCHEDULED"
  schedule = "cron(0 7 * * ? *)"

  actions {
    job_name = aws_glue_job.raw_to_bronze.name
  }
}
