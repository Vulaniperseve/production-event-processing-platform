terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # State is local (terraform.tfstate, git-ignored) for a single developer.
  # A team would use the S3 backend with native locking instead:
  #   backend "s3" { bucket = "...-tfstate", key = "lake.tfstate", use_lockfile = true }
}

provider "aws" {
  region = var.region

  # Every resource gets these tags: who owns it and that Terraform manages it.
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
