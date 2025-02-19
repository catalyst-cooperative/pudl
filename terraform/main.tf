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

resource "google_artifact_registry_repository" "pudl-superset-repo" {
  location      = "us-central1"
  repository_id = "pudl-superset"
  description   = "Docker image of PUDL superset deployment."
  format        = "docker"
}

resource "google_cloud_run_v2_service" "pudl-superset" {
  name     = "pudl-superset"
  location = "us-central1"
  client   = "terraform"

  launch_stage = "GA"

  template {
    execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
    containers {
      name  = "pudl-superset-1"
      image = "us-central1-docker.pkg.dev/catalyst-cooperative-pudl/pudl-superset/pudl-superset:latest"

      volume_mounts {
        name       = "bucket"
        mount_path = "/mnt/gcs"
      }
      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }
      env {
        name  = "IS_CLOUD_RUN"
        value = "True"
      }
      env {
        name = "SUPERSET_DB_USER"
        value_source {
          secret_key_ref {
            secret  = "superset-database-username"
            version = "1"
          }
        }
      }
      env {
        name = "SUPERSET_DB_NAME"
        value_source {
          secret_key_ref {
            secret  = "superset-database-database"
            version = "1"
          }
        }
      }
      env {
        name = "SUPERSET_DB_PASS"
        value_source {
          secret_key_ref {
            secret  = "superset-database-password"
            version = "1"
          }
        }
      }
      env {
        name = "SUPERSET_SECRET_KEY"
        value_source {
          secret_key_ref {
            secret  = "superset-secret-key"
            version = "1"
          }
        }
      }
      env {
        name = "CLOUD_SQL_CONNECTION_NAME"
        value_source {
          secret_key_ref {
            secret  = "superset-database-connection-name"
            version = "1"
          }
        }
      }
      env {
        name = "AUTH0_CLIENT_ID"
        value_source {
          secret_key_ref {
            secret  = "superset-auth0-client-id"
            version = "1"
          }
        }
      }
      env {
        name = "AUTH0_CLIENT_SECRET"
        value_source {
          secret_key_ref {
            secret  = "superset-auth0-client-secret"
            version = "2"
          }
        }
      }
      env {
        name = "AUTH0_DOMAIN"
        value_source {
          secret_key_ref {
            secret  = "superset-auth0-domain"
            version = "1"
          }
        }
      }
      env {
        name = "MAPBOX_API_KEY"
        value_source {
          secret_key_ref {
            secret  = "superset-mapbox-api-key"
            version = "1"
          }
        }
      }

      ports {
        container_port = 8088
      }
      resources {
        limits = {
          cpu    = "4"
          memory = "4096Mi"
        }
        startup_cpu_boost = true
      }
    }
    volumes {
      name = "bucket"
      gcs {
        bucket    = google_storage_bucket.superset_storage.name
        read_only = false
      }
    }
    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = ["catalyst-cooperative-pudl:us-central1:superset-database", "catalyst-cooperative-pudl:us-central1:pudl-usage-metrics-db"]
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "noauth" {
  location = google_cloud_run_v2_service.pudl-superset.location
  name     = google_cloud_run_v2_service.pudl-superset.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_secret_manager_secret" "superset_secret_key" {
  secret_id = "superset-secret-key"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "superset_mapbox_api_key" {
  secret_id = "superset-mapbox-api-key"
  replication {
    auto {}
  }
}

resource "google_sql_database_instance" "postgres_pvp_instance_name" {
  name             = "superset-database"
  region           = "us-central1"
  database_version = "POSTGRES_14"
  settings {
    tier = "db-custom-2-7680"
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

resource "google_secret_manager_secret" "superset_database_username" {
  secret_id = "superset-database-username"
  replication {
    auto {}
  }
}
resource "google_secret_manager_secret" "superset_database_database" {
  secret_id = "superset-database-database"
  replication {
    auto {}
  }
}
resource "google_secret_manager_secret" "superset_database_password" {
  secret_id = "superset-database-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "superset_database_connection_name" {
  secret_id = "superset-database-connection-name"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "superset_database_username_compute_iam" {
  secret_id = google_secret_manager_secret.superset_database_username.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_secret_manager_secret_iam_member" "superset_database_password_compute_iam" {
  secret_id = google_secret_manager_secret.superset_database_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_secret_manager_secret_iam_member" "superset_database_database_compute_iam" {
  secret_id = google_secret_manager_secret.superset_database_database.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_secret_manager_secret_iam_member" "superset_secret_key_compute_iam" {
  secret_id = google_secret_manager_secret.superset_secret_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_secret_manager_secret_iam_member" "superset_mapbox_api_key_compute_iam" {
  secret_id = google_secret_manager_secret.superset_mapbox_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_secret_manager_secret_iam_member" "superset_database_connection_name_compute_iam" {
  secret_id = google_secret_manager_secret.superset_database_connection_name.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "cloud_sql_client_role" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_secret_manager_secret" "superset_auth0_client_id" {
  secret_id = "superset-auth0-client-id"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "superset_auth0_client_id_compute_iam" {
  secret_id = google_secret_manager_secret.superset_auth0_client_id.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_secret_manager_secret" "superset_auth0_client_secret" {
  secret_id = "superset-auth0-client-secret"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "superset_auth0_client_secret_compute_iam" {
  secret_id = google_secret_manager_secret.superset_auth0_client_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_secret_manager_secret" "superset_auth0_domain" {
  secret_id = "superset-auth0-domain"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "superset_auth0_domain_compute_iam" {
  secret_id = google_secret_manager_secret.superset_auth0_domain.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_storage_bucket" "superset_storage" {
  name          = "superset.catalyst.coop"
  location      = "US"
  storage_class = "STANDARD"
}

resource "google_storage_bucket_iam_member" "superset_storage_compute_iam" {
  bucket = google_storage_bucket.superset_storage.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:345950277072-compute@developer.gserviceaccount.com"
}

resource "google_cloud_run_v2_service_iam_member" "cloudbuild_superset" {
  location = google_cloud_run_v2_service.pudl-superset.location
  name     = google_cloud_run_v2_service.pudl-superset.name
  role     = "roles/run.admin"
  member   = "serviceAccount:345950277072@cloudbuild.gserviceaccount.com"
}

data "google_compute_default_service_account" "google_compute_default_service_account_data" {
}

resource "google_service_account_iam_member" "gce-default-account-iam" {
  service_account_id = data.google_compute_default_service_account.google_compute_default_service_account_data.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:345950277072@cloudbuild.gserviceaccount.com"
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

resource "google_secret_manager_secret" "superset_bot_password" {
  secret_id = "superset-bot-password"
  replication {
    auto {}
  }
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

// PUDL Viewer config

locals {
  pudl_viewer_secret_versions = {
    pudl_viewer_secret_key          = 1
    pudl_viewer_db_username         = 1
    pudl_viewer_db_password         = 1
    pudl_viewer_db_name             = 1
    pudl_viewer_auth0_domain        = 1
    pudl_viewer_auth0_client_id     = 1
    pudl_viewer_auth0_client_secret = 1
  }
}

resource "google_service_account" "pudl_viewer_sa" {
  account_id   = "pudl-viewer-cloud-run"
  display_name = "PUDL Viewer Service Account"
}

resource "google_artifact_registry_repository" "pudl_viewer" {
  location      = "us-east1"
  repository_id = "pudl-viewer"
  description   = "Docker repository for PUDL viewer"
  format        = "DOCKER"
}

resource "google_sql_database_instance" "pudl_viewer_database" {
  name             = "pudl-viewer-database"
  region           = "us-central1"
  database_version = "POSTGRES_17"
  settings {
    tier      = "db-custom-1-3840"
    edition   = "ENTERPRISE"
    disk_size = 10
  }
  deletion_protection = true
}

resource "google_sql_database" "pudl_viewer_database" {
  name     = "pudl_viewer"
  instance = google_sql_database_instance.pudl_viewer_database.name
}

data "google_secret_manager_secret_version" "pudl_viewer_db_password" {
  secret  = "pudl_viewer_db_password"
  version = "1"
}

data "google_secret_manager_secret_version" "pudl_viewer_db_username" {
  secret  = "pudl_viewer_db_username"
  version = "1"
}

resource "google_sql_user" "user" {
  name     = data.google_secret_manager_secret_version.pudl_viewer_db_username.secret_data
  password = data.google_secret_manager_secret_version.pudl_viewer_db_password.secret_data
  instance = google_sql_database_instance.pudl_viewer_database.name
}


resource "google_cloud_run_v2_service" "pudl_viewer" {
  name                = "pudl-viewer"
  location            = "us-east1"
  deletion_protection = false

  template {
    annotations = {
      "client.knative.dev/user-image"     = "us-east1-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pudl_viewer.name}/pudl-viewer:latest"
      "run.googleapis.com/client-name"    = "terraform"
      "run.googleapis.com/client-version" = timestamp()
    }

    service_account = google_service_account.pudl_viewer_sa.email
    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [google_sql_database_instance.pudl_viewer_database.connection_name]
      }
    }

    containers {
      image = "us-east1-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pudl_viewer.name}/pudl-viewer:latest"

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      env {
        name  = "IS_CLOUD_RUN"
        value = "True"
      }

      env {
        name  = "CLOUD_SQL_CONNECTION_NAME"
        value = google_sql_database_instance.pudl_viewer_database.connection_name
      }

      dynamic "env" {
        for_each = local.pudl_viewer_secret_versions
        content {
          name = upper(env.key)
          value_source {
            secret_key_ref {
              secret  = env.key
              version = tostring(env.value)
            }
          }
        }
      }
    }
  }
}

resource "google_cloud_run_v2_job" "pudl_viewer_db_migration" {
  name                = "pudl-viewer-db-migration"
  location            = "us-east1"
  deletion_protection = false

  template {
    task_count = 1
    template {
      service_account = google_service_account.pudl_viewer_sa.email

      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [google_sql_database_instance.pudl_viewer_database.connection_name]
        }
      }

      containers {
        image   = "us-east1-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pudl_viewer.name}/pudl-viewer:latest"
        command = ["uv", "run", "flask", "--app", "parquet_fe_prototype", "db", "upgrade"]

        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }

        env {
          name  = "IS_CLOUD_RUN"
          value = "True"
        }

        env {
          name  = "CLOUD_SQL_CONNECTION_NAME"
          value = google_sql_database_instance.pudl_viewer_database.connection_name
        }

        dynamic "env" {
          for_each = local.pudl_viewer_secret_versions
          content {
            name = upper(env.key)
            value_source {
              secret_key_ref {
                secret  = env.key
                version = tostring(env.value)
              }
            }
          }
        }
      }
    }
  }
}


resource "google_cloud_run_v2_service_iam_member" "pudl_viewer_public" {
  location = google_cloud_run_v2_service.pudl_viewer.location
  name     = google_cloud_run_v2_service.pudl_viewer.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_secret_manager_secret" "pudl_viewer_secrets" {
  for_each  = local.pudl_viewer_secret_versions
  secret_id = each.key
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "pudl_viewer_secret_accessor" {
  for_each  = google_secret_manager_secret.pudl_viewer_secrets
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.pudl_viewer_sa.member
}

resource "google_project_iam_member" "pudl_viewer_cloud_sql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = google_service_account.pudl_viewer_sa.member
}


// PUDL viewer log sink
resource "google_storage_bucket" "pudl_viewer_logs" {
  name          = "pudl-viewer-logs.catalyst.coop"
  location      = "US"
  storage_class = "STANDARD"

  lifecycle_rule {
    condition {
      age = 180
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_logging_project_sink" "pudl_viewer_log_sink" {
  name        = "pudl-viewer-log-sink"
  description = "Move PUDL viewer logs into Cloud Storage for longer persistence."
  destination = "storage.googleapis.com/${google_storage_bucket.pudl_viewer_logs.name}"
  filter      = <<EOT
    resource.type = "cloud_run_revision"
    AND resource.labels.service_name="${google_cloud_run_v2_service.pudl_viewer.name}"
    AND (severity >= DEFAULT)
  EOT

  unique_writer_identity = true
}

resource "google_storage_bucket_iam_member" "pudl_viewer_log_writer" {
  bucket = google_storage_bucket.pudl_viewer_logs.name
  role   = "roles/storage.objectCreator"

  member = google_logging_project_sink.pudl_viewer_log_sink.writer_identity
}
