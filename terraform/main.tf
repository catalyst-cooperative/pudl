terraform {
  backend "gcs" {
    bucket = "f3441e415e6e5e7d-bucket-tfstate"
    prefix = "terraform/state"
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.14.1"
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
    "nrel-finito-inputs-gha" = {
      sa_name   = "projects/${var.project_id}/serviceAccounts/${google_service_account.nrel_finito_inputs_gha.email}"
      attribute = "attribute.repository/catalyst-cooperative/nrel-fuel-and-industry-inputs"
    }
    "pudl-usage-metrics-dashboard-deploy-gha" = {
      sa_name   = "projects/${var.project_id}/serviceAccounts/${google_service_account.pudl_usage_metrics_dashboard_deploy_gha.email}"
      attribute = "attribute.repository/catalyst-cooperative/pudl-usage-metrics-dashboard"
    }
  }
}

# Generate a random password for the mlflow db user
resource "random_password" "mlflow_postgresql_password" {
  length  = 16   # Adjust the password length as needed
  special = true # Include special characters
  upper   = true # Include uppercase letters
  lower   = true # Include lowercase letters
  numeric = true # Include numbers
}

# Create secret to store mlflow db password
resource "google_secret_manager_secret" "mlflow_postgresql_password_secret" {
  secret_id = "mlflow-postgresql-password"
  replication {
    auto {}
  }
}

# Create version of secret with mlflow password set
resource "google_secret_manager_secret_version" "mlflow_postgresql_password_version" {
  secret      = google_secret_manager_secret.mlflow_postgresql_password_secret.id
  secret_data = random_password.mlflow_postgresql_password.result
}

# Create mlflow postgresql instance for backend storage
resource "google_sql_database_instance" "mlflow_backend_store" {
  name             = "mlflow-backend-store"
  region           = "us-central1"
  database_version = "POSTGRES_14"
  settings {
    tier = "db-f1-micro"
    activation_policy = "NEVER"
    password_validation_policy {
      min_length                  = 6
      reuse_interval              = 2
      complexity                  = "COMPLEXITY_DEFAULT"
      disallow_username_substring = true
      password_change_interval    = "30s"
      enable_password_policy      = true
    }

  }
  # set `deletion_protection` to true, will ensure that one cannot accidentally delete this instance by
  # use of Terraform whereas `deletion_protection_enabled` flag protects this instance at the GCP level.
  deletion_protection = true
}

resource "google_storage_bucket" "pudl_models_outputs" {
  name          = "model-outputs.catalyst.coop"
  location      = "US"
  storage_class = "STANDARD"
}

resource "google_sql_user" "mlflow_postgresql_user" {
  name     = "postgres"
  instance = google_sql_database_instance.mlflow_backend_store.name
  password = random_password.mlflow_postgresql_password.result
}

# Optional: Create a database in the PostgreSQL instance
resource "google_sql_database" "mlflow_postgresql_database" {
  name     = "mlflow"
  instance = google_sql_database_instance.mlflow_backend_store.name
}

resource "google_project_iam_member" "cloud_sql_client_role" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_secret_manager_secret" "pudl_usage_metrics_db_connection_string" {
  secret_id = "pudl-usage-metrics-db-connection-string"
  replication {
    auto {}
  }
}

resource "google_storage_bucket" "pudl_usage_metrics_archive_bucket" {
  name          = "pudl-usage-metrics-archives.catalyst.coop"
  location      = "US"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
}

resource "google_service_account" "usage_metrics_archiver" {
  account_id   = "usage-metrics-archiver"
  display_name = "PUDL usage metrics archiver github action service account"
}

resource "google_storage_bucket_iam_member" "usage_metrics_archiver_gcs_iam" {
  for_each = toset(["roles/storage.objectCreator", "roles/storage.objectViewer", "roles/storage.insightsCollectorService"])

  bucket = google_storage_bucket.pudl_usage_metrics_archive_bucket.name
  role   = each.key
  member = "serviceAccount:${google_service_account.usage_metrics_archiver.email}"
}

resource "google_storage_bucket_iam_member" "usage_metrics_etl_gcs_iam" {
  for_each = toset(["roles/storage.legacyBucketReader", "roles/storage.objectViewer"])

  bucket = google_storage_bucket.pudl_usage_metrics_archive_bucket.name
  role   = each.key
  member = "serviceAccount:pudl-usage-metrics-etl@catalyst-cooperative-pudl.iam.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "usage_metrics_etl_s3_logs_gcs_iam" {
  for_each = toset(["roles/storage.legacyBucketReader", "roles/storage.objectViewer"])

  bucket = "pudl-s3-logs.catalyst.coop"
  role   = each.key
  member = "serviceAccount:pudl-usage-metrics-etl@catalyst-cooperative-pudl.iam.gserviceaccount.com"
}

resource "google_storage_bucket" "pudl_archive_bucket" {
  name          = "archives.catalyst.coop"
  location      = "US-EAST1"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
}

resource "google_service_account" "nrel_finito_inputs_gha" {
  account_id   = "nrel-finito-inputs-gha"
  display_name = "NREL FINITO inputs github action service account"
}

resource "google_storage_bucket_iam_member" "nrel_finito_inputs_archiver_gcs_iam" {
  for_each = toset([
    "roles/storage.objectCreator",
    "roles/storage.objectViewer",
    "roles/storage.insightsCollectorService"
  ])

  bucket = google_storage_bucket.pudl_archive_bucket.name
  role   = each.key
  member = "serviceAccount:${google_service_account.nrel_finito_inputs_gha.email}"
}
