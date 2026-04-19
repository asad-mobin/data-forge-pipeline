provider "aws" {
    region = "eu-west-2"
}

resource "aws_s3_bucket" "dataforge_bucket" {
    bucket = "dataforge-bronze-asad"
}
