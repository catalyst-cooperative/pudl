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
    "pudl-tox-pytest-github-action" = {
      sa_name   = "projects/${var.project_id}/serviceAccounts/tox-pytest-github-action@catalyst-cooperative-pudl.iam.gserviceaccount.com"
      attribute = "attribute.repository/catalyst-cooperative/pudl"
    }
    "pudl-deploy-pudl-github-action" = {
      sa_name   = "projects/${var.project_id}/serviceAccounts/deploy-pudl-github-action@catalyst-cooperative-pudl.iam.gserviceaccount.com"
      attribute = "attribute.repository/catalyst-cooperative/pudl"
    }
    "pudl-zenodo-cache-manager" = {
      sa_name   = "projects/${var.project_id}/serviceAccounts/zenodo-cache-manager@catalyst-cooperative-pudl.iam.gserviceaccount.com"
      attribute = "attribute.repository/catalyst-cooperative/pudl"
    }
    "pudl-usage-metrics-pudl-usage-metrics-etl" = {
      sa_name   = "projects/${var.project_id}/serviceAccounts/pudl-usage-metrics-etl@catalyst-cooperative-pudl.iam.gserviceaccount.com"
      attribute = "attribute.repository/catalyst-cooperative/pudl-usage-metrics"
    }
    "pudl-catalog-tox-pytest-github-action" = {
      sa_name   = "projects/${var.project_id}/serviceAccounts/tox-pytest-github-action@catalyst-cooperative-pudl.iam.gserviceaccount.com"
      attribute = "attribute.repository/catalyst-cooperative/pudl-catalog"
    }
    "gce-build-test-gce-github-action-test" = {
      sa_name   = "projects/${var.project_id}/serviceAccounts/gce-github-action-test@catalyst-cooperative-pudl.iam.gserviceaccount.com"
      attribute = "attribute.repository/catalyst-cooperative/gce-build-test"
    }
    "sec-extraction-test-github-action" = {
      sa_name   = "projects/catalyst-cooperative-mozilla/serviceAccounts/mozilla-dev-sa@catalyst-cooperative-mozilla.iam.gserviceaccount.com"
      attribute = "attribute.repository/catalyst-cooperative/mozilla-sec-eia"
    }
  }
}

# 2024-04-18: separate from the others because this was the first one - if we
# combined the two, this would delete and recreate the service account
resource "google_service_account" "service_account" {
  account_id   = "rmi-beta-access"
  display_name = "rmi_beta_access"
}

# 2024-04-18: after creating a new SA you will have to also create a keypair
# for the user.
resource "google_service_account" "beta_access_service_accounts" {
  for_each = tomap({
    zerolab_beta_access  = "zerolab-beta-access"
    gridpath_beta_access = "gridpath-beta-access"
  })
  account_id   = each.value
  display_name = each.key
}

resource "google_storage_bucket_iam_binding" "binding" {
  bucket = "parquet.catalyst.coop"
  role   = "roles/storage.objectViewer"
  members = [
    "serviceAccount:rmi-beta-access@catalyst-cooperative-pudl.iam.gserviceaccount.com",
    "serviceAccount:zerolab-beta-access@catalyst-cooperative-pudl.iam.gserviceaccount.com",
    "serviceAccount:gridpath-beta-access@catalyst-cooperative-pudl.iam.gserviceaccount.com",
    "serviceAccount:dgm-github-action@dbcp-dev-350818.iam.gserviceaccount.com",
    "user:aengel@rmi.org",
  ]
}
