"""Integration tests for the silver/gold transforms on a real (local) Spark session.

Pins the genuinely-hard logic: dedup-to-latest-prediction, epoch-UTC → local-service-day
reconciliation, the after-midnight (>24:00:00) wrap correction, and the OTP on-time band.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql.types import (IntegerType, LongType, StructField, StructType, StringType)

from src.transforms.lateness import compute_lateness
from src.transforms.otp import classify, otp_agg

NY = ZoneInfo("America/New_York")


def _epoch(local_str: str) -> int:
    """Local NY wall-clock string → UTC epoch seconds (what RT actually stores)."""
    return int(datetime.strptime(local_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=NY).timestamp())


RT_SCHEMA = StructType([
    StructField("trip_id", StringType()), StructField("stop_id", StringType()),
    StructField("arrival_time", LongType()), StructField("feed_ts", LongType()),
    StructField("schedule_relationship", IntegerType()),
])
SCHED_SCHEMA = StructType([
    StructField("trip_id", StringType()), StructField("stop_id", StringType()),
    StructField("sched_secs", LongType()),
])


def _lateness(spark, rt_rows, sched_rows):
    rt = spark.createDataFrame(rt_rows, RT_SCHEMA)
    sched = spark.createDataFrame(sched_rows, SCHED_SCHEMA)
    return {r["trip_id"]: r for r in compute_lateness(rt, sched).collect()}


def test_lateness_uses_latest_prediction_and_is_correct(spark):
    # two snapshots for the same (trip,stop); the later feed_ts (08:05) must win → 5 min late.
    rt = [("T1", "S1", _epoch("2026-06-20 08:04:00"), 100, 0),
          ("T1", "S1", _epoch("2026-06-20 08:05:00"), 200, 0)]
    out = _lateness(spark, rt, [("T1", "S1", 8 * 3600)])  # scheduled 08:00:00
    assert out["T1"]["lateness_min"] == 5.0


def test_early_arrival_is_negative(spark):
    rt = [("T2", "S1", _epoch("2026-06-20 07:58:00"), 100, 0)]
    out = _lateness(spark, rt, [("T2", "S1", 8 * 3600)])
    assert out["T2"]["lateness_min"] == -2.0


def test_after_midnight_wrap_correction(spark):
    # schedule 24:30:00 (88200s, after-midnight service); actual arrives 00:35 next local day.
    # Without the wrap fix this reads as hugely early; with it, 5 min late.
    rt = [("T3", "S1", _epoch("2026-06-21 00:35:00"), 100, 0)]
    out = _lateness(spark, rt, [("T3", "S1", 24 * 3600 + 30 * 60)])
    assert out["T3"]["lateness_min"] == 5.0


def test_non_scheduled_and_null_arrivals_are_dropped(spark):
    rt = [("T4", "S1", _epoch("2026-06-20 08:05:00"), 100, 1),   # schedule_relationship != 0
          ("T5", "S1", None, 100, 0)]                              # null arrival
    out = _lateness(spark, rt, [("T4", "S1", 8 * 3600), ("T5", "S1", 8 * 3600)])
    assert out == {}


def test_otp_band_classification_and_rollup(spark):
    rows = [("A", 2.0, 29100), ("A", 10.0, 29100), ("A", -3.0, 29100), ("A", 0.0, 29100)]
    df = spark.createDataFrame(rows, "route string, lateness_min double, actual_secs_adj long")
    agg = {r["route"]: r for r in otp_agg(classify(df), ["route"]).collect()}["A"]
    assert (agg["observations"], agg["on_time_n"], agg["late_n"], agg["early_n"]) == (4, 2, 1, 1)
    assert agg["otp_pct"] == 50.0                       # 2 of 4 on-time
    assert agg["on_time_n"] + agg["late_n"] + agg["early_n"] == agg["observations"]


def test_hour_derived_from_local_seconds(spark):
    rows = [("A", 1.0, 8 * 3600 + 300)]                 # 08:05 → hour 8
    df = spark.createDataFrame(rows, "route string, lateness_min double, actual_secs_adj long")
    assert classify(df).collect()[0]["hour"] == 8
