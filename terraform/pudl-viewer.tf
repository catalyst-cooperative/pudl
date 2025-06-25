// secrets
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

resource "google_secret_manager_secret" "pudl_viewer_secrets" {
  for_each  = local.pudl_viewer_secret_versions
  secret_id = each.key
  replication {
    auto {}
  }
}

// cloud run service account & permissions
resource "google_service_account" "pudl_viewer_sa" {
  account_id   = "pudl-viewer-cloud-run"
  display_name = "PUDL Viewer Service Account"
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

// artifacts
resource "google_artifact_registry_repository" "pudl_viewer" {
  location      = "us-east1"
  repository_id = "pudl-viewer"
  description   = "Docker repository for PUDL viewer"
  format        = "DOCKER"
}


// user DB
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


// the service itself
resource "google_cloud_run_v2_service" "pudl_viewer" {
  name                = "pudl-viewer"
  location            = "us-east1"
  deletion_protection = false

  scaling {
    min_instance_count = 1
  }

  template {
    annotations = {
      "client.knative.dev/user-image"  = "us-east1-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pudl_viewer.name}/pudl-viewer:latest"
      "run.googleapis.com/client-name" = "terraform"
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

      resources {
        limits = {
          cpu    = "1000m"
          memory = "768Mi"
        }
        cpu_idle = true
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
        command = ["uv", "run", "flask", "--app", "eel_hole", "db", "upgrade"]

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


// public accessibility
resource "google_cloud_run_v2_service_iam_member" "pudl_viewer_public" {
  location = google_cloud_run_v2_service.pudl_viewer.location
  name     = google_cloud_run_v2_service.pudl_viewer.name
  role     = "roles/run.invoker"
  member   = "allUsers"
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
