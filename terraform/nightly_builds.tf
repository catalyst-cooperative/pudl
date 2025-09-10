// nightly build service account
resource "google_service_account" "nightly_build" {
  account_id   = "deploy-pudl-vm-service-account"
  display_name = "Batch Build Service Account"
  // this was once used for the builds on the deploy-pudl VM. but now we just use it for Batch.
  description = "This service account is used by the nightly and branch PUDL builds."
}

// allow nightly builds to deploy cloud run service (for pudl viewer)
resource "google_project_iam_member" "nightly_build_cloud_run" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = google_service_account.nightly_build.member
}

// need to be able to *use* service accounts too, since the pudl viewer service SA is different from the nightly build SA
resource "google_project_iam_member" "nightly_build_use_cloud_run_sa" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = google_service_account.nightly_build.member
}
