provider "google" {
  project = var.project_id
  region  = var.region
}

# Raw GTFS-Realtime object landing (bronze). `force_destroy = true` keeps the
# "tear infra down between sessions" principle cheap — teardown won't block on objects.
resource "google_storage_bucket" "bronze_rt" {
  name                        = "${var.project_id}-bronze-rt"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

# GTFS-Realtime ingestion topic; the poller publishes pings here, Databricks consumes.
resource "google_pubsub_topic" "gtfs_rt" {
  name = var.topic_name
}
