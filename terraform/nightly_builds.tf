// nightly build service account
resource "google_service_account" "nightly_build" {
  account_id   = "deploy-pudl-vm-service-account"
  display_name = "Batch Build Service Account"
  // this was once used for the builds on the deploy-pudl VM. but now we just use it for Batch.
  description = "This service account is used by the nightly and branch PUDL builds."
}

resource "google_project_iam_member" "nightly_build" {
  for_each = toset([
    "roles/run.developer",          // update cloud run services
    "roles/iam.serviceAccountUser", // the cloud run service can use a service account that is different from the nightly build one
  ])
  project = var.project_id
  role    = each.key
  member  = google_service_account.nightly_build.member
}
