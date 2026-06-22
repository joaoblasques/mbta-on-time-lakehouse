"""Copy new GTFS-RT .pb snapshots from GCS into the Databricks managed Volume.

Free Edition can't read GCS directly (WIF gated), so GCP *pushes* via the Databricks Files API.
**Incremental**: only scans the last COPY_DAYS date-partitions and diffs each partition with a
single Volume directory listing (not a per-file metadata call) — bounded work regardless of how
much history has accumulated, so it can't hit the Cloud Run timeout. Idempotent. Read-only on GCS.

Env: GCS_BRONZE_BUCKET, DATABRICKS_HOST, DATABRICKS_TOKEN, COPY_DAYS (default 2).
"""

from __future__ import annotations

import datetime as dt
import io
import os

from google.cloud import storage
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

BUCKET = os.environ["GCS_BRONZE_BUCKET"]
VOL_ROOT = "/Volumes/mbta/bronze/rt_raw"
FEEDS = ("vehicle_positions", "trip_updates", "alerts")
COPY_DAYS = int(os.environ.get("COPY_DAYS", "2"))


def _volume_filenames(w: WorkspaceClient, directory: str) -> set[str]:
    """One listing of a Volume dir → set of filenames already present (empty if dir absent)."""
    try:
        return {e.path.rsplit("/", 1)[-1] for e in w.files.list_directory_contents(directory)}
    except NotFound:
        return set()


def main() -> None:
    gcs = storage.Client()
    w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
    bucket = gcs.bucket(BUCKET)
    today = dt.date.today()
    dates = [(today - dt.timedelta(days=i)).isoformat() for i in range(COPY_DAYS)]

    uploaded = skipped = 0
    for feed in FEEDS:
        for d in dates:
            prefix = f"{feed}/dt={d}/"
            existing = _volume_filenames(w, f"{VOL_ROOT}/{prefix}")
            for blob in bucket.list_blobs(prefix=prefix):
                if not blob.name.endswith(".pb"):
                    continue
                fn = blob.name.rsplit("/", 1)[-1]
                if fn in existing:
                    skipped += 1
                    continue
                w.files.upload(f"{VOL_ROOT}/{blob.name}", io.BytesIO(blob.download_as_bytes()),
                               overwrite=False)
                uploaded += 1

    print(f"copy complete: uploaded={uploaded} skipped={skipped} "
          f"(last {COPY_DAYS}d, bucket={BUCKET})")


if __name__ == "__main__":
    main()
