# --- GCS -> Databricks Volume copy job (closes the loop) ---
# Free Edition can't read GCS directly, so this Cloud Run Job pushes new .pb snapshots
# into the managed Volume via the Databricks Files API. Scheduled; idempotent.

variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL the copy job pushes to."
  default     = "https://dbc-b39edf4c-7929.cloud.databricks.com"
}

variable "copy_schedule" {
  type        = string
  description = "Cron for the GCS->Volume copy job (UTC)."
  default     = "*/15 * * * *" # every 15 minutes
}

resource "google_project_service" "secretmanager" {
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

# Runtime identity for the copy job: read the bucket + read the Databricks token secret.
resource "google_service_account" "copier" {
  account_id   = "mbta-copier"
  display_name = "GCS -> Databricks Volume copy job"
}

resource "google_storage_bucket_iam_member" "copier_reader" {
  bucket = google_storage_bucket.bronze_rt.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.copier.email}"
}

# The databricks-token secret is created out-of-band (gcloud) to keep it out of TF state.
resource "google_secret_manager_secret_iam_member" "copier_secret" {
  secret_id  = "databricks-token"
  role       = "roles/secretmanager.secretAccessor"
  member     = "serviceAccount:${google_service_account.copier.email}"
  depends_on = [google_project_service.secretmanager]
}

resource "google_cloud_run_v2_job" "copier" {
  name                = "mbta-copier"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.copier.email
      max_retries     = 1
      timeout         = "300s"
      containers {
        image   = "${var.region}-docker.pkg.dev/${var.project_id}/mbta/poller:latest"
        command = ["python", "-m", "src.ingestion.gcs_to_volume"]
        env {
          name  = "GCS_BRONZE_BUCKET"
          value = google_storage_bucket.bronze_rt.name
        }
        env {
          name  = "DATABRICKS_HOST"
          value = var.databricks_host
        }
        env {
          name = "DATABRICKS_TOKEN"
          value_source {
            secret_key_ref {
              secret  = "databricks-token"
              version = "latest"
            }
          }
        }
      }
    }
  }
  depends_on = [google_project_service.apis, google_secret_manager_secret_iam_member.copier_secret]
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoke_copier" {
  name     = google_cloud_run_v2_job.copier.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "copier" {
  name      = "mbta-copier"
  region    = var.region
  schedule  = var.copy_schedule
  time_zone = "Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/mbta-copier:run"
    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
  depends_on = [google_project_service.apis, google_cloud_run_v2_job.copier]
}

output "copier_job" {
  value       = google_cloud_run_v2_job.copier.name
  description = "Cloud Run Job that copies GCS RT snapshots into the Databricks Volume."
}
