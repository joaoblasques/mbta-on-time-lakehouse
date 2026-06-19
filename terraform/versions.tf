terraform {
  required_version = ">= 1.12.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Remote state in GCS (bucket bootstrapped imperatively — it can't manage the bucket
  # that stores its own state). Versioning is enabled on the bucket for state history.
  backend "gcs" {
    bucket = "mbta-on-time-lakehouse-tfstate"
    prefix = "terraform/state"
  }
}
