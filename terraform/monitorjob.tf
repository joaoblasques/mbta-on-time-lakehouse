# --- Failure-monitor: self-healing half of the loop (Cloud Run Job + Scheduler) ---
# Reads medallion run history; Tier-1 auto-retry on a fresh failure, Tier-2 deduped GitHub
# issue once retries are exhausted. Read-mostly (only pipeline write is re-running a job).

variable "medallion_job_id" {
  type        = string
  description = "Databricks job id of mbta-medallion-refresh (monitored). Bundle-managed job."
  default     = "390935525530573"
}

variable "monitor_schedule" {
  type        = string
  description = "Cron for the failure-monitor (UTC)."
  default     = "*/30 * * * *"
}

resource "google_service_account" "monitor" {
  account_id   = "mbta-monitor"
  display_name = "Failure-monitor (self-heal: retry / escalate)"
}

resource "google_secret_manager_secret_iam_member" "monitor_dbx" {
  secret_id  = "databricks-token"
  role       = "roles/secretmanager.secretAccessor"
  member     = "serviceAccount:${google_service_account.monitor.email}"
  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_iam_member" "monitor_github" {
  secret_id  = "github-token"
  role       = "roles/secretmanager.secretAccessor"
  member     = "serviceAccount:${google_service_account.monitor.email}"
  depends_on = [google_project_service.secretmanager]
}

resource "google_cloud_run_v2_job" "monitor" {
  name                = "mbta-monitor"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.monitor.email
      max_retries     = 0
      timeout         = "120s"
      containers {
        image   = "${var.region}-docker.pkg.dev/${var.project_id}/mbta/poller:latest"
        command = ["python", "-m", "src.monitor.run_monitor"]
        env {
          name  = "DATABRICKS_HOST"
          value = var.databricks_host
        }
        env {
          name  = "MEDALLION_JOB_ID"
          value = var.medallion_job_id
        }
        env {
          name  = "GITHUB_REPO"
          value = "joaoblasques/mbta-on-time-lakehouse"
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
          name = "GITHUB_TOKEN"
          value_source {
            secret_key_ref {
              secret  = "github-token"
              version = "latest"
            }
          }
        }
      }
    }
  }
  depends_on = [
    google_project_service.apis,
    google_secret_manager_secret_iam_member.monitor_dbx,
    google_secret_manager_secret_iam_member.monitor_github,
  ]
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoke_monitor" {
  name     = google_cloud_run_v2_job.monitor.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "monitor" {
  name      = "mbta-monitor"
  region    = var.region
  schedule  = var.monitor_schedule
  time_zone = "Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/mbta-monitor:run"
    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
  depends_on = [google_project_service.apis, google_cloud_run_v2_job.monitor]
}

output "monitor_job" {
  value       = google_cloud_run_v2_job.monitor.name
  description = "Cloud Run Job: failure-monitor (self-heal)."
}
