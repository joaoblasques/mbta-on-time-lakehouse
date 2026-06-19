"""Copy new GTFS-RT .pb snapshots from GCS into the Databricks managed Volume.

Free Edition can't read GCS directly (Workload Identity Federation is gated), so GCP must
*push*: list GCS objects and upload any not already present in the Volume, via the Databricks
Files API. Idempotent (skips files already in the Volume). Runs as a scheduled Cloud Run Job.

Env: GCS_BRONZE_BUCKET, DATABRICKS_HOST, DATABRICKS_TOKEN.
"""

from __future__ import annotations

import io
import os

from google.cloud import storage
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

BUCKET = os.environ["GCS_BRONZE_BUCKET"]
VOL_ROOT = "/Volumes/mbta/bronze/rt_raw"


def main() -> None:
    gcs = storage.Client()
    w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
    bucket = gcs.bucket(BUCKET)

    uploaded = skipped = 0
    for blob in bucket.list_blobs():
        if not blob.name.endswith(".pb"):
            continue
        dest = f"{VOL_ROOT}/{blob.name}"  # mirror the GCS path under the Volume
        try:
            w.files.get_metadata(dest)
            skipped += 1
            continue  # already in the Volume → idempotent skip
        except NotFound:
            pass
        w.files.upload(dest, io.BytesIO(blob.download_as_bytes()), overwrite=False)
        uploaded += 1

    print(f"copy complete: uploaded={uploaded} skipped={skipped} (bucket={BUCKET})")


if __name__ == "__main__":
    main()
