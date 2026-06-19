variable "project_id" {
  type        = string
  description = "GCP project ID (single identity: tilakapash@gmail.com owns it)."
}

variable "region" {
  type        = string
  description = "GCP region for regional resources."
  default     = "us-east1"
}

variable "topic_name" {
  type        = string
  description = "Pub/Sub topic the GTFS-Realtime poller publishes to."
  default     = "mbta-gtfs-rt"
}
