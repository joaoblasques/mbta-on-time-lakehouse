"""Shared OTP mart builders (transforms.otp.by_route / by_route_hour / by_stop).

Both the Jobs notebook (05_gold_otp.py) and the DLT notebook (dlt_otp_marts.py) call these
exact functions, so pinning them here guarantees the two paradigms produce identical marts.
"""

from src.transforms.otp import by_route, by_route_hour, by_stop

# silver.trip_stop_lateness-shaped columns the builders need. actual_secs_adj 29100 = 08:05 → hour 8.
SILVER = ("route_id string, route_short_name string, route_long_name string, "
          "stop_id string, stop_name string, lateness_min double, actual_secs_adj long")


def _rows():
    # R1/S1: 4 observations → 2 on-time (2.0, 0.0), 1 late (10.0), 1 early (-3.0) → OTP 50%.
    return [
        ("R1", "1", "Route One", "S1", "Stop S1", 2.0, 29100),
        ("R1", "1", "Route One", "S1", "Stop S1", 10.0, 29100),
        ("R1", "1", "Route One", "S1", "Stop S1", -3.0, 29100),
        ("R1", "1", "Route One", "S1", "Stop S1", 0.0, 29100),
    ]


def test_by_route_rolls_up_otp(spark):
    df = spark.createDataFrame(_rows(), SILVER)
    r = {x["route_id"]: x for x in by_route(df).collect()}["R1"]
    assert (r["observations"], r["on_time_n"], r["late_n"], r["early_n"]) == (4, 2, 1, 1)
    assert r["otp_pct"] == 50.0


def test_by_route_hour_derives_hour(spark):
    df = spark.createDataFrame(_rows(), SILVER)
    r = by_route_hour(df).collect()[0]
    assert (r["route_id"], r["hour"], r["observations"]) == ("R1", 8, 4)


def test_by_stop_applies_min_obs_filter(spark):
    df = spark.createDataFrame(_rows(), SILVER)
    assert by_stop(df, min_obs=20).count() == 0          # 4 obs < 20 → dropped
    kept = {x["stop_id"]: x for x in by_stop(df, min_obs=1).collect()}["S1"]
    assert kept["observations"] == 4 and kept["otp_pct"] == 50.0
