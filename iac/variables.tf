variable "region_id" {
  default = "sa-east-1"
}

variable "nome_bucket" {
  default = "igti-datalake-astheobaldo"
}

variable "conta_aws" {
  default = "120131503669"
}

variable "prefix" {
  default = "igti-datalake-astheobaldo"
}

# Prefix configuration and project common tags
locals {
  prefix = "${var.prefix}-${terraform.workspace}"
  common_tags = {
    Project      = "Projeto Aplicado"
    ManagedBy    = "Terraform"
  }
}
variable "bucket_names" {
  description = "Create S3 buckets with these names"
  type        = list(string)
  default = [
    "raw",
    "bronze",
    "silver",
    "gold"
  ]
}