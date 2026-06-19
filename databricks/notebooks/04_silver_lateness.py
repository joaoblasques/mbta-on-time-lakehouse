# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver: lateness (actual RT arrival vs. scheduled)
# MAGIC Joins bronze `rt_trip_updates` (actual/predicted arrival, **epoch UTC**) to silver
# MAGIC `stop_times` (scheduled **seconds-after-midnight, local service day**) → minutes late
# MAGIC per trip/stop. Idempotent. Writes `mbta.silver.trip_stop_lateness`.
# MAGIC
# MAGIC **The hard part (handled):** the two times live in different worlds —
# MAGIC RT is an absolute UTC instant, the schedule is a local time-of-day that can exceed
# MAGIC 24:00:00 (after-midnight service). We convert RT → local service-day seconds and
# MAGIC correct the midnight wrap.
# MAGIC
# MAGIC **Honest caveat:** `trip_updates.arrival_time` is often a *prediction*; with only a
# MAGIC ~5-min capture window we use the **latest prediction per (trip, stop)** as the
# MAGIC actual-proxy. Real OTP would confirm against observed arrival over a full day.

# COMMAND ----------
from pyspark.sql import functions as F, Window
TZ = "America/New_York"

# COMMAND ----------
# 1) Dedup RT: many snapshots per (trip,stop) → keep the LATEST prediction (max feed_ts).
rt = (spark.table("mbta.bronze.rt_trip_updates")
        .filter(F.col("arrival_time").isNotNull() & (F.col("schedule_relationship") == 0)))  # 0 = SCHEDULED
w = Window.partitionBy("trip_id", "stop_id").orderBy(F.col("feed_ts").desc())
rt_latest = rt.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

# COMMAND ----------
# 2) Actual arrival epoch → local wall-clock → seconds-after-local-midnight + service_date.
rt_local = (rt_latest
    .withColumn("actual_local_ts", F.from_utc_timestamp(F.col("arrival_time").cast("timestamp"), TZ))
    .withColumn("service_date", F.to_date("actual_local_ts"))
    .withColumn("actual_secs",
                F.hour("actual_local_ts")*3600 + F.minute("actual_local_ts")*60 + F.second("actual_local_ts")))

# COMMAND ----------
# 3) Join to the schedule and compute lateness (with after-midnight wrap correction).
sched = (spark.table("mbta.silver.stop_times")
            .filter(F.col("arrival_secs").isNotNull())
            .select("trip_id", "stop_id", F.col("arrival_secs").alias("sched_secs")))

lateness = (rt_local.join(sched, ["trip_id", "stop_id"], "inner")
    # GTFS may schedule >24h (e.g. 24:30:00). If actual wrapped past local midnight, lift it a day.
    .withColumn("actual_secs_adj",
        F.when((F.col("sched_secs") >= 86400) & (F.col("actual_secs") < F.col("sched_secs") - 43200),
               F.col("actual_secs") + 86400).otherwise(F.col("actual_secs")))
    .withColumn("lateness_secs", F.col("actual_secs_adj") - F.col("sched_secs"))
    .withColumn("lateness_min", F.round(F.col("lateness_secs") / 60.0, 1)))

# COMMAND ----------
# 4) Enrich with route + stop names for readability.
routes = spark.table("mbta.silver.routes").select("route_id", "route_short_name", "route_long_name")
stops  = spark.table("mbta.silver.stops").select("stop_id", "stop_name")
trips  = spark.table("mbta.silver.trips").select("trip_id", "direction_id")

out = (lateness
    .join(routes, "route_id", "left")
    .join(stops, "stop_id", "left")
    .join(trips, "trip_id", "left")
    .select("service_date", "route_id", "route_short_name", "route_long_name",
            "trip_id", "direction_id", "stop_id", "stop_name", "stop_sequence",
            "sched_secs", "actual_secs_adj", "lateness_secs", "lateness_min", "feed_ts")
    .withColumn("_ingested_at", F.current_timestamp()))

out.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.silver.trip_stop_lateness")
print("trip_stop_lateness rows:", spark.table("mbta.silver.trip_stop_lateness").count())

# COMMAND ----------
# MAGIC %md ## DQ gates + a first look at the answer
def dq(check, ok):
    print(("OK  " if ok else "FAIL"), check); assert ok, f"DQ FAILED: {check}"

L = spark.table("mbta.silver.trip_stop_lateness")
dq("lateness table non-empty", L.count() > 0)
dq("lateness computed (not all null)", L.filter(F.col("lateness_min").isNotNull()).count() > 0)
# sanity band: flag, don't fail, extreme outliers (data is a 5-min predictive window)
outliers = L.filter(F.abs(F.col("lateness_min")) > 120).count()
print(f"(info) |lateness| > 120 min rows: {outliers}")

# COMMAND ----------
# MAGIC %md ### Gold preview — avg lateness by route (this is what 05 will formalize)
display(
    L.groupBy("route_id", "route_short_name", "route_long_name")
     .agg(F.count("*").alias("observations"),
          F.round(F.avg("lateness_min"), 1).alias("avg_late_min"),
          F.round(F.expr("percentile(lateness_min, 0.5)"), 1).alias("median_late_min"))
     .orderBy(F.desc("avg_late_min"))
)
