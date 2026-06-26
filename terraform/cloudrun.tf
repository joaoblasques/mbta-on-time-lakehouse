# --- Scheduled GTFS-Realtime poller: Cloud Run Job triggered by Cloud Scheduler ---
# The poller container runs one poll per invocation; Scheduler fires it on a cadence so
# RT snapshots accumulate into the bronze bucket → real OTP history.

variable "poll_schedule" {
  type        = string
  description = "Cron for the poller (UTC)."
  default     = "*/2 * * * *" # every 2 minutes
}

# Enable the APIs this stack needs.
resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# Docker repo for the poller image.
resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = "mbta"
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}

# Runtime identity for the poller; least-privilege write to the bronze bucket only.
resource "google_service_account" "poller" {
  account_id   = "mbta-poller"
  display_name = "MBTA GTFS-RT poller (Cloud Run Job)"
}

resource "google_storage_bucket_iam_member" "poller_writer" {
  bucket = google_storage_bucket.bronze_rt.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.poller.email}"
}

# The poller job.
resource "google_cloud_run_v2_job" "poller" {
  name                = "mbta-poller"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.poller.email
      max_retries     = 1
      timeout         = "120s"
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/mbta/poller:latest"
        env {
          name  = "GCS_BRONZE_BUCKET"
          value = google_storage_bucket.bronze_rt.name
        }
      }
    }
  }
  depends_on = [google_project_service.apis, google_artifact_registry_repository.images]
}

# Identity Cloud Scheduler uses to trigger the job.
resource "google_service_account" "scheduler" {
  account_id   = "mbta-scheduler"
  display_name = "Triggers the MBTA poller job"
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker" {
  name     = google_cloud_run_v2_job.poller.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

# Fire the job on a cadence (calls the Run Admin API :run endpoint with the scheduler SA).
resource "google_cloud_scheduler_job" "poller" {
  name      = "mbta-poller"
  region    = var.region
  schedule  = var.poll_schedule
  time_zone = "Etc/UTC"
  paused    = true # cost teardown 2026-06-25: keep dormant; flip to false to resume

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/mbta-poller:run"
    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
  depends_on = [google_project_service.apis, google_cloud_run_v2_job.poller]
}

output "poller_job" {
  value       = google_cloud_run_v2_job.poller.name
  description = "Cloud Run Job that polls GTFS-RT."
}

output "poller_schedule" {
  value       = google_cloud_scheduler_job.poller.schedule
  description = "Cadence the poller runs on (UTC cron)."
}
