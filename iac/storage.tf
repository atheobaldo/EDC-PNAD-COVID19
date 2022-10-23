resource "aws_s3_bucket" "buckets" {
  count  = length(var.bucket_names)
  bucket = "${local.prefix}-datalake-${var.bucket_names[count.index]}"
  acl    = "private"
  tags = local.common_tags

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
}