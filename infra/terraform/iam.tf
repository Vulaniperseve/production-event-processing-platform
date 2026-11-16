# Least-privilege identity for the pipeline (Airflow publish_to_s3).
#
# It can list, read, write and delete objects under <bucket>/<lake_prefix>/
# and nothing else: no other prefix, no other bucket, no bucket settings,
# no other AWS service.
#
# The access key is NOT created here. Terraform would store the secret in
# its state file in plain text. Create it once in the IAM console (or with
# `aws iam create-access-key --user-name <name>`) and put it in airflow/.env.

resource "aws_iam_user" "lake_publisher" {
  name = "${var.project}-lake-publisher"
}

data "aws_iam_policy_document" "lake_publisher" {
  statement {
    sid       = "ListLakePrefixOnly"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.lake_prefix}/*"]
    }
  }

  statement {
    sid = "ReadWriteLakeObjects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["${aws_s3_bucket.lake.arn}/${var.lake_prefix}/*"]
  }
}

resource "aws_iam_policy" "lake_publisher" {
  name        = "${var.project}-lake-publisher"
  description = "Sync the local data lake to s3://${local.bucket_name}/${var.lake_prefix}/"
  policy      = data.aws_iam_policy_document.lake_publisher.json
}

resource "aws_iam_user_policy_attachment" "lake_publisher" {
  user       = aws_iam_user.lake_publisher.name
  policy_arn = aws_iam_policy.lake_publisher.arn
}
