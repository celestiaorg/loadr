# Disposable demo infrastructure — local state is deliberate: the whole stack
# lives for a few hours and `terraform destroy` is part of the demo. Point the
# backend at your state bucket instead if you want durability:
#
# terraform {
#   backend "s3" {
#     bucket = "<project>-terraform-state-<account-id>"
#     key    = "demos/2m-rps/terraform.tfstate"
#     region = "eu-west-2"
#   }
# }

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "loadr"
      Demo      = "2m-rps"
      ManagedBy = "terraform"
      Ephemeral = "true"
    }
  }
}
