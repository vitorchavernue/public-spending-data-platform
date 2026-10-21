# IAM role assumed by the Snowflake storage integration to read the bronze layer.
# Apply once with empty variables, create the integration in Snowflake, then fill
# snowflake_iam_user_arn / snowflake_external_id (DESC INTEGRATION) and apply again.

locals {
  snowflake_principal = var.snowflake_iam_user_arn != "" ? var.snowflake_iam_user_arn : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
}

data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "snowflake_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [local.snowflake_principal]
    }
    dynamic "condition" {
      for_each = var.snowflake_external_id != "" ? [1] : []
      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [var.snowflake_external_id]
      }
    }
  }
}

resource "aws_iam_role" "snowflake" {
  name               = "psdp-snowflake-storage-integration"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume.json
}

data "aws_iam_policy_document" "snowflake_read" {
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.data_lake.arn}/bronze/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data_lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["bronze/*"]
    }
  }
}

resource "aws_iam_role_policy" "snowflake_read" {
  name   = "bronze-read-only"
  role   = aws_iam_role.snowflake.id
  policy = data.aws_iam_policy_document.snowflake_read.json
}
