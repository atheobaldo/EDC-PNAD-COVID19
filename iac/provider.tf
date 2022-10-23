provider "aws" {
  region = var.region_id
}

# Centralizar o arquivo de controle de estado do terraform
# Criar bucket manualmente na AWS
terraform {
  backend "s3" {
    bucket = "igti-terraform-atheobaldo"
    key    = "terraform/state/terraform.tfstate"
    region = var.region_id
  }
}
