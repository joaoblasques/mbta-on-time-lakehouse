"""Silver: compute minutes-late by reconciling RT actual arrivals (epoch UTC) with the schedule
(seconds-after-local-midnight). The hard parts — dedup to the latest prediction, epoch→local
service-day seconds, and the after-midnight (>24:00:00) wrap correction — live here, tested.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TZ = "America/New_York"
HALF_DAY, DAY = 43200, 86400  # seconds


def latest_rt(rt: DataFrame) -> DataFrame:
    """Keep the latest prediction per (trip, stop): scheduled-relationship rows with an arrival,
    deduped by max feed_ts. `rt` cols: trip_id, stop_id, arrival_time (epoch s), feed_ts,
    schedule_relationship."""
    scheduled = rt.filter(F.col("arrival_time").isNotNull() & (F.col("schedule_relationship") == 0))
    w = Window.partitionBy("trip_id", "stop_id").orderBy(F.col("feed_ts").desc())
    return scheduled.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def compute_lateness(rt: DataFrame, sched: DataFrame, tz: str = TZ) -> DataFrame:
    """rt (raw RT trip_updates) ⋈ sched (trip_id, stop_id, sched_secs) → lateness per trip/stop.

    Returns: trip_id, stop_id, service_date, sched_secs, actual_secs_adj, lateness_secs,
    lateness_min, feed_ts. Late > 0, early < 0.
    """
    local = (latest_rt(rt)
             .withColumn("actual_local_ts", F.from_utc_timestamp(F.col("arrival_time").cast("timestamp"), tz))
             .withColumn("service_date", F.to_date("actual_local_ts"))
             .withColumn("actual_secs",
                         F.hour("actual_local_ts") * 3600
                         + F.minute("actual_local_ts") * 60
                         + F.second("actual_local_ts")))

    return (local.join(sched, ["trip_id", "stop_id"], "inner")
            # GTFS schedules can exceed 24:00:00 (after-midnight service). If the actual wrapped
            # past local midnight, lift it a day so the subtraction stays in the same frame.
            .withColumn("actual_secs_adj",
                        F.when((F.col("sched_secs") >= DAY)
                               & (F.col("actual_secs") < F.col("sched_secs") - HALF_DAY),
                               F.col("actual_secs") + DAY).otherwise(F.col("actual_secs")))
            .withColumn("lateness_secs", F.col("actual_secs_adj") - F.col("sched_secs"))
            .withColumn("lateness_min", F.round(F.col("lateness_secs") / 60.0, 1))
            .select("trip_id", "stop_id", "service_date", "sched_secs", "actual_secs_adj",
                    "lateness_secs", "lateness_min", "feed_ts"))
