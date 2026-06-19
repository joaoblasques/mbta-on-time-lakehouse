# Container for the GTFS-Realtime poller, run as a Cloud Run Job (one poll per invocation;
# Cloud Scheduler triggers it on a cadence). Auth is the job's runtime service account (ADC).
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir \
    "requests>=2.32" \
    "gtfs-realtime-bindings>=1.0" \
    "google-cloud-storage>=2.16" \
    "databricks-sdk>=0.30"

COPY src ./src

# Default = one-shot poll. The GCS->Volume copy Job overrides the command to run
# src.ingestion.gcs_to_volume instead (same image, two jobs).
ENTRYPOINT ["python", "-m", "src.ingestion.gtfs_rt_poller"]
