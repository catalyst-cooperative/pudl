// secrets
locals {
  metrics_dashboard_secret_versions = {
    metrics_dashboard_password    = 1
    metrics_dashboard_db_name     = 1
    metrics_dashboard_db_username = 1
    metrics_dashboard_db_password = 1
  }
}

resource "google_secret_manager_secret" "metrics_dashboard_secrets" {
  for_each  = local.metrics_dashboard_secret_versions
  secret_id = each.key
  replication {
    auto {}
  }
}

// Deployment: SA and artifact registry
resource "google_service_account" "metrics_dashboard_deploy_gha" {
  account_id   = "metrics-dashboard-deploy-gha"
  display_name = "SA for metrics dashboard deployment"
}

resource "google_project_iam_member" "metrics_dashboard_artifact_registry" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.metrics_dashboard_deploy_gha.email}"
}

resource "google_project_iam_member" "metrics_dashboard_cloud_run_deploy" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.metrics_dashboard_deploy_gha.email}"
}

resource "google_project_iam_member" "metrics_dashboard_deploy_use_cloud_run" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.metrics_dashboard_deploy_gha.email}"
}

resource "google_artifact_registry_repository" "metrics_dashboard" {
  location      = "us-east1"
  repository_id = "metrics-dashboard"
  description   = "Docker repository for metrics dashboard"
  format        = "DOCKER"
}


// the actual cloud run SA
resource "google_service_account" "metrics_dashboard_cloud_run" {
  account_id   = "metrics-dash"
  display_name = "Metrics Dashboard Service Account"
}

resource "google_secret_manager_secret_iam_member" "metrics_dashboard_secret_accessor" {
  for_each  = google_secret_manager_secret.metrics_dashboard_secrets
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.metrics_dashboard_cloud_run.member
}

resource "google_project_iam_member" "metrics_dashboard_cloud_sql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = google_service_account.metrics_dashboard_cloud_run.member
}

// read-only SQL user

data "google_secret_manager_secret_version" "metrics_dashboard_db_password" {
  secret  = "metrics_dashboard_db_password"
  version = "1"
}

data "google_secret_manager_secret_version" "metrics_dashboard_db_username" {
  secret  = "metrics_dashboard_db_username"
  version = "1"
}

resource "google_sql_user" "metrics_dashboard_readonly" {
  name     = data.google_secret_manager_secret_version.metrics_dashboard_db_username.secret_data
  password = data.google_secret_manager_secret_version.metrics_dashboard_db_password.secret_data
  instance = "pudl-usage-metrics-db"
}


// cloud run service
resource "google_cloud_run_v2_service" "metrics_dashboard" {
  name                = "metrics-dashboard"
  location            = "us-east1"
  deletion_protection = false

  scaling {
    min_instance_count = 1
  }

  template {
    annotations = {
      "run.googleapis.com/client-name" = "terraform"
    }


    service_account = google_service_account.metrics_dashboard_cloud_run.email
    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = ["catalyst-cooperative-pudl:us-central1:pudl-usage-metrics-db"]
      }
    }

    containers {
      image = "us-east1-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.metrics_dashboard.name}/metrics-dashboard:latest"

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
        for_each = local.metrics_dashboard_secret_versions
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


resource "google_cloud_run_v2_service_iam_member" "metrics_dashboard_internet" {
  location = google_cloud_run_v2_service.metrics_dashboard.location
  name     = google_cloud_run_v2_service.metrics_dashboard.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
