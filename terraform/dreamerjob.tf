# --- OTP Dreamer: nightly sleep-time analysis (Cloud Run Job + Scheduler) ---
# Reads gold via the Databricks SQL API, narrates via OpenRouter, writes an insight note +
# learned baseline to GCS. Read-only (never touches the pipeline).

variable "warehouse_id" {
  type        = string
  description = "Databricks serverless SQL warehouse id the dreamer queries."
  default     = "e2d0993979faf3d2"
}

variable "dreamer_schedule" {
  type        = string
  description = "Cron for the nightly dreamer (UTC)."
  default     = "30 6 * * *"
}

resource "google_service_account" "dreamer" {
  account_id   = "mbta-dreamer"
  display_name = "OTP Dreamer (nightly sleep-time analysis)"
}

# read+write the _dreamer/ area of the bronze bucket (baseline + insight notes)
resource "google_storage_bucket_iam_member" "dreamer_gcs" {
  bucket = google_storage_bucket.bronze_rt.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dreamer.email}"
}

# secrets: the Databricks PAT (gold SQL) + the OpenRouter key (LLM)
resource "google_secret_manager_secret_iam_member" "dreamer_dbx" {
  secret_id  = "databricks-token"
  role       = "roles/secretmanager.secretAccessor"
  member     = "serviceAccount:${google_service_account.dreamer.email}"
  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_iam_member" "dreamer_openrouter" {
  secret_id  = "openrouter-api-key"
  role       = "roles/secretmanager.secretAccessor"
  member     = "serviceAccount:${google_service_account.dreamer.email}"
  depends_on = [google_project_service.secretmanager]
}

resource "google_cloud_run_v2_job" "dreamer" {
  name                = "mbta-dreamer"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.dreamer.email
      max_retries     = 1
      timeout         = "300s"
      containers {
        image   = "${var.region}-docker.pkg.dev/${var.project_id}/mbta/poller:latest"
        command = ["python", "-m", "src.dreamer.run_dreamer"]
        env {
          name  = "DATABRICKS_HOST"
          value = var.databricks_host
        }
        env {
          name  = "DATABRICKS_WAREHOUSE_ID"
          value = var.warehouse_id
        }
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.bronze_rt.name
        }
        env {
          name  = "OPENROUTER_MODEL"
          value = "openai/gpt-oss-120b:free"
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
        env {
          name = "OPENROUTER_API_KEY"
          value_source {
            secret_key_ref {
              secret  = "openrouter-api-key"
              version = "latest"
            }
          }
        }
      }
    }
  }
  depends_on = [
    google_project_service.apis,
    google_secret_manager_secret_iam_member.dreamer_dbx,
    google_secret_manager_secret_iam_member.dreamer_openrouter,
  ]
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoke_dreamer" {
  name     = google_cloud_run_v2_job.dreamer.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "dreamer" {
  name      = "mbta-dreamer"
  region    = var.region
  schedule  = var.dreamer_schedule
  time_zone = "Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/mbta-dreamer:run"
    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
  depends_on = [google_project_service.apis, google_cloud_run_v2_job.dreamer]
}

output "dreamer_job" {
  value       = google_cloud_run_v2_job.dreamer.name
  description = "Cloud Run Job: nightly OTP Dreamer."
}
