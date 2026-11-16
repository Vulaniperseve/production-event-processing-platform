variable "project" {
  description = "Name prefix for every resource."
  type        = string
  default     = "event-platform"

  validation {
    condition     = can(regex("^[a-z0-9-]{3,30}$", var.project))
    error_message = "Use 3-30 lowercase letters, digits or hyphens (it becomes part of the bucket name)."
  }
}

variable "region" {
  description = "AWS region. af-south-1 (Cape Town) keeps data in South Africa but must first be enabled in the account settings."
  type        = string
  default     = "eu-west-1"
}

variable "lake_prefix" {
  description = "Key prefix of the data lake inside the bucket (LAKE_S3_PREFIX)."
  type        = string
  default     = "lake"
}

variable "symbols" {
  description = "Symbols the gold daily_metrics table is partitioned by (Athena partition projection)."
  type        = list(string)
  default     = ["AAPL", "MSFT", "GOOGL"]
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget; alerts fire at 50% actual and 100% forecast."
  type        = number
  default     = 5
}

variable "alert_email" {
  description = "Where budget alerts go. Set it in terraform.tfvars (git-ignored), never in code."
  type        = string
}

variable "athena_bytes_scanned_limit" {
  description = "Athena cancels any query that would scan more than this (cost guard). Minimum 10 MB."
  type        = number
  default     = 104857600 # 100 MB
}
