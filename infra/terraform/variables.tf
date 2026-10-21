variable "region" {
  type    = string
  default = "us-east-1"
}

variable "data_lake_bucket" {
  description = "Globally unique name of the data lake bucket"
  type        = string
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query scan limit in the Athena workgroup (cost guardrail)"
  type        = number
  default     = 10737418240 # 10 GB
}

variable "snowflake_iam_user_arn" {
  description = "STORAGE_AWS_IAM_USER_ARN from DESC INTEGRATION S3_DATA_LAKE"
  type        = string
  default     = ""
}

variable "snowflake_external_id" {
  description = "STORAGE_AWS_EXTERNAL_ID from DESC INTEGRATION S3_DATA_LAKE"
  type        = string
  default     = ""
}
