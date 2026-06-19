"""Unit tests for the GTFS-RT poller's pure logic (no network, no GCP)."""

import datetime as dt

import pytest
from google.transit import gtfs_realtime_pb2

from src.ingestion.gtfs_rt_poller import feed_entity_count, snapshot_path


def test_snapshot_path_is_date_partitioned_and_timestamped():
    ts = dt.datetime(2026, 6, 19, 14, 25, 30, tzinfo=dt.timezone.utc)
    assert snapshot_path("vehicle_positions", ts) == (
        "vehicle_positions/dt=2026-06-19/vehicle_positions_20260619T142530Z.pb"
    )


def test_snapshot_paths_differ_by_second_so_reruns_dont_overwrite():
    base = dt.datetime(2026, 6, 19, 14, 25, 30, tzinfo=dt.timezone.utc)
    later = base + dt.timedelta(seconds=1)
    assert snapshot_path("trip_updates", base) != snapshot_path("trip_updates", later)


def _make_feed(n_entities: int) -> bytes:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    for i in range(n_entities):
        msg.entity.add().id = f"e{i}"
    return msg.SerializeToString()


def test_feed_entity_count_parses_valid_feed():
    assert feed_entity_count(_make_feed(3)) == 3


def test_feed_entity_count_rejects_garbage():
    # An HTML error page / truncated body must NOT pass the gate.
    with pytest.raises(Exception):
        feed_entity_count(b"<html>503 Service Unavailable</html>")
