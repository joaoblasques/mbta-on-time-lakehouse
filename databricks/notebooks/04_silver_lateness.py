# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver: lateness (actual RT arrival vs. scheduled)
# MAGIC Joins bronze `rt_trip_updates` (actual/predicted arrival, **epoch UTC**) to silver
# MAGIC `stop_times` (scheduled **seconds-after-midnight, local service day**) → minutes late
# MAGIC per trip/stop, then enriches with route/stop names. Writes `mbta.silver.trip_stop_lateness`.
# MAGIC
# MAGIC **The hard logic lives in the tested wheel** `transforms.lateness.compute_lateness`
# MAGIC (dedup-to-latest, epoch-UTC→local-service-day, the >24:00:00 after-midnight wrap) — this
# MAGIC notebook is just I/O + enrichment around it. See `docs/testing.md`. Idempotent.

# COMMAND ----------
import datetime as dt

from pyspark.sql import functions as F

from transforms.lateness import compute_lateness  # tested wheel (deployed by the Asset Bundle)

WINDOW_DAYS = 3  # bronze is now full-history (Auto Loader append); window silver to keep it bounded

# COMMAND ----------
# Inputs: bronze RT actuals (recent window only) + the schedule (seconds-after-local-midnight).
since_epoch = int((dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=WINDOW_DAYS)).timestamp())
rt = spark.table("mbta.bronze.rt_trip_updates").filter(F.col("feed_ts") >= since_epoch)
sched = (spark.table("mbta.silver.stop_times")
         .filter(F.col("arrival_secs").isNotNull())
         .select("trip_id", "stop_id", F.col("arrival_secs").alias("sched_secs")))

lateness = compute_lateness(rt, sched)   # trip_id, stop_id, service_date, sched_secs,
#                                          actual_secs_adj, lateness_secs, lateness_min, feed_ts

# COMMAND ----------
# Enrich for readability: route_id + direction from trips, names from routes/stops.
trips = spark.table("mbta.silver.trips").select("trip_id", "route_id", "direction_id")
routes = spark.table("mbta.silver.routes").select("route_id", "route_short_name", "route_long_name")
stops = spark.table("mbta.silver.stops").select("stop_id", "stop_name")

out = (lateness
       .join(trips, "trip_id", "left")
       .join(routes, "route_id", "left")
       .join(stops, "stop_id", "left")
       .select("service_date", "route_id", "route_short_name", "route_long_name",
               "trip_id", "direction_id", "stop_id", "stop_name",
               "sched_secs", "actual_secs_adj", "lateness_secs", "lateness_min", "feed_ts")
       .withColumn("_ingested_at", F.current_timestamp()))

out.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.silver.trip_stop_lateness")
print("trip_stop_lateness rows:", spark.table("mbta.silver.trip_stop_lateness").count())

# COMMAND ----------
# MAGIC %md ## DQ gates
def dq(check, ok):
    print(("OK  " if ok else "FAIL"), check); assert ok, f"DQ FAILED: {check}"


L = spark.table("mbta.silver.trip_stop_lateness")
dq("lateness table non-empty", L.count() > 0)
dq("lateness computed (not all null)", L.filter(F.col("lateness_min").isNotNull()).count() > 0)
print(f"(info) |lateness| > 120 min rows: {L.filter(F.abs(F.col('lateness_min')) > 120).count()}")
display(
    L.groupBy("route_id", "route_short_name", "route_long_name")
     .agg(F.count("*").alias("observations"),
          F.round(F.avg("lateness_min"), 1).alias("avg_late_min"),
          F.round(F.expr("percentile(lateness_min, 0.5)"), 1).alias("median_late_min"))
     .orderBy(F.desc("avg_late_min"))
)
