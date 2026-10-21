output "data_lake_bucket" {
  value = aws_s3_bucket.data_lake.id
}

output "glue_job_name" {
  value = aws_glue_job.raw_to_bronze.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.analytics.name
}

output "snowflake_s3_role_arn" {
  description = "Use as STORAGE_AWS_ROLE_ARN in snowflake/02_load_bronze.sql"
  value       = aws_iam_role.snowflake.arn
}
