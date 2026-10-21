# Workgroup with an enforced per-query scan limit: a query that would scan more than
# the cutoff is cancelled instead of generating an unexpected bill.
resource "aws_athena_workgroup" "analytics" {
  name = "psdp-analytics"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.id}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
