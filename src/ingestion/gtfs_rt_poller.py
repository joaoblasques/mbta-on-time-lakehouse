"""GTFS-Realtime poller: fetch MBTA RT feeds and land raw protobuf snapshots in GCS (bronze).

I/O (HTTP fetch, GCS upload) is separated from pure logic (path building, feed validation)
so the logic is unit-testable without network or cloud access.

Run one-shot:   uv run python -m src.ingestion.gtfs_rt_poller
Run a loop:     uv run python -m src.ingestion.gtfs_rt_poller --interval 15 --iterations 20
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import time

import requests
from google.transit import gtfs_realtime_pb2

# MBTA GTFS-Realtime feeds — public .pb endpoints (no API key needed for these raw feeds).
FEEDS = {
    "vehicle_positions": "https://cdn.mbta.com/realtime/VehiclePositions.pb",
    "trip_updates": "https://cdn.mbta.com/realtime/TripUpdates.pb",
    "alerts": "https://cdn.mbta.com/realtime/Alerts.pb",
}

DEFAULT_BUCKET = "mbta-on-time-lakehouse-bronze-rt"


# --- pure logic (unit-testable, no I/O) -------------------------------------------------

def snapshot_path(feed: str, ts: dt.datetime) -> str:
    """Object path for one snapshot, date-partitioned for cheap pruning. Pure.

    e.g. vehicle_positions/dt=2026-06-19/vehicle_positions_20260619T142530Z.pb
    The timestamp in the name makes re-runs idempotent (no overwrite of a prior poll).
    """
    day = ts.strftime("%Y-%m-%d")
    stamp = ts.strftime("%Y%m%dT%H%M%SZ")
    return f"{feed}/dt={day}/{feed}_{stamp}.pb"


def feed_entity_count(data: bytes) -> int:
    """Parse GTFS-RT protobuf and return the entity count. Pure.

    Raises on bytes that aren't a valid FeedMessage — the DQ gate that stops a truncated
    response or an HTML error page from being landed as if it were a real snapshot.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(data)
    return len(msg.entity)


# --- I/O --------------------------------------------------------------------------------

def fetch_feed(url: str, api_key: str | None = None, timeout: int = 15) -> bytes:
    headers = {"x-api-key": api_key} if api_key else {}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def upload_snapshot(bucket, path: str, data: bytes) -> None:
    bucket.blob(path).upload_from_string(data, content_type="application/x-protobuf")


def poll_once(bucket, feeds: dict[str, str] = FEEDS, api_key: str | None = None,
              now: dt.datetime | None = None) -> dict[str, int]:
    """Fetch each feed once, validate, upload. Returns {feed: entity_count}."""
    ts = now or dt.datetime.now(dt.timezone.utc)
    counts: dict[str, int] = {}
    for feed, url in feeds.items():
        data = fetch_feed(url, api_key)
        n = feed_entity_count(data)  # raises -> we skip uploading garbage
        upload_snapshot(bucket, snapshot_path(feed, ts), data)
        counts[feed] = n
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll MBTA GTFS-RT into the GCS bronze bucket.")
    parser.add_argument("--bucket", default=os.environ.get("GCS_BRONZE_BUCKET", DEFAULT_BUCKET))
    parser.add_argument("--interval", type=int, default=0,
                        help="Seconds between polls; 0 = single one-shot poll.")
    parser.add_argument("--iterations", type=int, default=1,
                        help="Polls to run when interval>0 (0 = run forever).")
    args = parser.parse_args()

    from google.cloud import storage  # imported here so pure-logic tests need no GCP libs

    api_key = os.environ.get("MBTA_API_KEY") or None
    bucket = storage.Client().bucket(args.bucket)

    i = 0
    while True:
        counts = poll_once(bucket, api_key=api_key)
        print(f"landed snapshot -> gs://{args.bucket}: {counts}")
        i += 1
        if args.interval <= 0 or (args.iterations and i >= args.iterations):
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
