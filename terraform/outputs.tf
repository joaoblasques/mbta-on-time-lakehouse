output "bronze_rt_bucket" {
  description = "GCS bucket for raw GTFS-Realtime landing (bronze)."
  value       = google_storage_bucket.bronze_rt.url
}

output "gtfs_rt_topic" {
  description = "Fully-qualified Pub/Sub topic id the poller publishes to."
  value       = google_pubsub_topic.gtfs_rt.id
}
