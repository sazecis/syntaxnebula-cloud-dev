provider "aws" {
  region = "<your-region>"
}

resource "aws_s3_bucket" "my_bucket" {
  bucket               = "<bucket-unique-name>"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.my_bucket.id

  topic {
    topic_arn     = "<topic-arn>"
    events        = ["s3:ObjectCreated:*"]
  }
}