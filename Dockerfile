# Container for the GTFS-Realtime poller, run as a Cloud Run Job (one poll per invocation;
# Cloud Scheduler triggers it on a cadence). Auth is the job's runtime service account (ADC).
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir \
    "requests>=2.32" \
    "gtfs-realtime-bindings>=1.0" \
    "google-cloud-storage>=2.16"

COPY src ./src

# Default args = one-shot poll (interval=0, iterations=1). Bucket via env (set by the Job).
ENTRYPOINT ["python", "-m", "src.ingestion.gtfs_rt_poller"]
