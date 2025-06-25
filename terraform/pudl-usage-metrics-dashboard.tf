// secrets
locals {
  pudl_usage_metrics_dashboard_secret_versions = {
    pudl_usage_metrics_dashboard_password    = 1
    pudl_usage_metrics_dashboard_db_name     = 1
    pudl_usage_metrics_dashboard_db_username = 1
    pudl_usage_metrics_dashboard_db_password = 1
  }
}

resource "google_secret_manager_secret" "pudl_usage_metrics_dashboard_secrets" {
  for_each  = local.pudl_usage_metrics_dashboard_secret_versions
  secret_id = each.key
  replication {
    auto {}
  }
}

// Deployment: SA and artifact registry
resource "google_service_account" "pudl_usage_metrics_dashboard_deploy_gha" {
  account_id   = "pudl-usage-metrics-dash-deploy"
  display_name = "SA for metrics dashboard deployment"
}

resource "google_project_iam_member" "pudl_usage_metrics_dashboard_artifact_registry" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.pudl_usage_metrics_dashboard_deploy_gha.email}"
}

resource "google_project_iam_member" "pudl_usage_metrics_dashboard_cloud_run_deploy" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.pudl_usage_metrics_dashboard_deploy_gha.email}"
}

resource "google_project_iam_member" "pudl_usage_metrics_dashboard_deploy_use_cloud_run" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.pudl_usage_metrics_dashboard_deploy_gha.email}"
}

resource "google_artifact_registry_repository" "pudl_usage_metrics_dashboard" {
  location      = "us-east1"
  repository_id = "pudl-usage-metrics-dashboard"
  description   = "Docker repository for metrics dashboard"
  format        = "DOCKER"
}


// the actual cloud run SA
resource "google_service_account" "pudl_usage_metrics_dashboard_cloud_run" {
  account_id   = "pudl-usage-metrics-dashboard"
  display_name = "PUDL Usage Metrics Dashboard Service Account"
}

resource "google_secret_manager_secret_iam_member" "pudl_usage_metrics_dashboard_secret_accessor" {
  for_each  = google_secret_manager_secret.pudl_usage_metrics_dashboard_secrets
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.pudl_usage_metrics_dashboard_cloud_run.member
}

resource "google_project_iam_member" "pudl_usage_metrics_dashboard_cloud_sql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = google_service_account.pudl_usage_metrics_dashboard_cloud_run.member
}

// read-only SQL user

data "google_secret_manager_secret_version" "pudl_usage_metrics_dashboard_db_password" {
  secret  = "pudl_usage_metrics_dashboard_db_password"
  version = "1"
}

data "google_secret_manager_secret_version" "pudl_usage_metrics_dashboard_db_username" {
  secret  = "pudl_usage_metrics_dashboard_db_username"
  version = "1"
}

resource "google_sql_user" "pudl_usage_metrics_dashboard_readonly" {
  name     = data.google_secret_manager_secret_version.pudl_usage_metrics_dashboard_db_username.secret_data
  password = data.google_secret_manager_secret_version.pudl_usage_metrics_dashboard_db_password.secret_data
  instance = "pudl-usage-metrics-db"
}


// cloud run service
resource "google_cloud_run_v2_service" "pudl_usage_metrics_dashboard" {
  name                = "pudl-usage-metrics-dashboard"
  location            = "us-east1"
  deletion_protection = false

  scaling {
    min_instance_count = 1
  }

  template {
    annotations = {
      "run.googleapis.com/client-name" = "terraform"
    }


    service_account = google_service_account.pudl_usage_metrics_dashboard_cloud_run.email
    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = ["catalyst-cooperative-pudl:us-central1:pudl-usage-metrics-db"]
      }
    }

    containers {
      image = "us-east1-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pudl_usage_metrics_dashboard.name}/pudl-usage-metrics-dashboard:latest"

      resources {
        limits = {
          cpu    = "1000m"
          memory = "2Gi"
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
        value = "catalyst-cooperative-pudl:us-central1:pudl-usage-metrics-db"
      }

      dynamic "env" {
        for_each = local.pudl_usage_metrics_dashboard_secret_versions
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


resource "google_cloud_run_v2_service_iam_member" "pudl_usage_metrics_dashboard_internet" {
  location = google_cloud_run_v2_service.pudl_usage_metrics_dashboard.location
  name     = google_cloud_run_v2_service.pudl_usage_metrics_dashboard.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
