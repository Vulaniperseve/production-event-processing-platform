# The data lake bucket.
#
# Name = project + account id: globally unique, and the same on every apply.

locals {
  bucket_name    = "${var.project}-lake-${data.aws_caller_identity.current.account_id}"
  athena_results = "athena-results"
}

resource "aws_s3_bucket" "lake" {
  bucket = local.bucket_name
}

# Nothing in this bucket is ever public.
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ACLs off: access is controlled only by IAM and the bucket policy.
resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Encryption at rest with S3-managed keys (free). SSE-KMS would add a key
# cost and change ETags, which the sync compares with local MD5s.
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning: a bad sync (or a mistaken delete) can be undone by restoring
# the previous object version.
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Cost control. Old versions are kept for 7 days, which is long enough to
# recover from a mistake, and Athena results are temporary.
# There is no transition to STANDARD_IA: these objects are a few KB, and IA
# bills every object as at least 128 KB for at least 30 days, so it would cost more.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # Lifecycle rules on a versioned bucket need versioning to exist first.
  depends_on = [aws_s3_bucket_versioning.lake]

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
    expiration {
      expired_object_delete_marker = true
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "${local.athena_results}/"
    }

    expiration {
      days = 7
    }
  }
}

# Refuse any request that is not over TLS.
data "aws_iam_policy_document" "lake_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.lake.arn,
      "${aws_s3_bucket.lake.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = data.aws_iam_policy_document.lake_bucket.json

  # Applying a policy while the public access block is still being created can fail.
  depends_on = [aws_s3_bucket_public_access_block.lake]
}
