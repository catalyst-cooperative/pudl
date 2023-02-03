terraform {
  backend "gcs" {
    bucket = "f3441e415e6e5e7d-bucket-tfstate"
    prefix = "terraform/state"
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

variable "project_id" {
  type    = string
  default = "catalyst-cooperative-pudl"
}

provider "google" {
  project = var.project_id
  region  = "us-east1"
  zone    = "us-east1-c"
}

resource "random_id" "bucket_prefix" {
  byte_length = 8
}

resource "google_storage_bucket" "tfstate" {
  name          = "${random_id.bucket_prefix.hex}-bucket-tfstate"
  force_destroy = false
  location      = "US"
  storage_class = "STANDARD"
  versioning {
    enabled = true
  }
}

module "gh_oidc" {
  source      = "terraform-google-modules/github-actions-runners/google//modules/gh-oidc"
  project_id  = var.project_id
  pool_id     = "gh-actions-pool"
  provider_id = "gh-actions-provider"
  sa_mapping = {
    "tox-pytest-github-action-service-account" = {
      sa_name   = "projects/${var.project_id}/serviceAccounts/tox-pytest-github-action@catalyst-cooperative-pudl.iam.gserviceaccount.com"
      attribute = "*"
    }
  }
}
