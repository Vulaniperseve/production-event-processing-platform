output "lake_bucket" {
  description = "Put this in airflow/.env as LAKE_S3_BUCKET."
  value       = aws_s3_bucket.lake.bucket
}

output "lake_uri" {
  value = "s3://${aws_s3_bucket.lake.bucket}/${var.lake_prefix}/"
}

output "publisher_user" {
  description = "Create this user's access key in the IAM console and put it in airflow/.env."
  value       = aws_iam_user.lake_publisher.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lake.name
}

output "glue_database" {
  value = aws_glue_catalog_database.lake.name
}
