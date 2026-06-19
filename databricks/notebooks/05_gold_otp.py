# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold: On-Time Performance (OTP) marts
# MAGIC From `silver.trip_stop_lateness` → the headline metric, **OTP %**, as three marts:
# MAGIC - `mbta.gold.otp_by_route` — OTP per route (the scorecard)
# MAGIC - `mbta.gold.otp_by_route_hour` — OTP per route × hour-of-day (when does it degrade?)
# MAGIC - `mbta.gold.otp_by_stop` — OTP per stop (**where delays concentrate**)
# MAGIC
# MAGIC **"On time" is a product decision**, not a given: here a stop is on-time if lateness is
# MAGIC within `[EARLY_BOUND, LATE_BOUND]` minutes. Tune per mode. Idempotent (overwrite).

# COMMAND ----------
from pyspark.sql import functions as F
spark.sql("CREATE SCHEMA IF NOT EXISTS mbta.gold")

EARLY_BOUND, LATE_BOUND = -1.0, 5.0   # minutes: < EARLY = early, > LATE = late, else on-time

L = (spark.table("mbta.silver.trip_stop_lateness")
     .filter(F.col("lateness_min").isNotNull())
     .withColumn("on_time", ((F.col("lateness_min") >= EARLY_BOUND) &
                             (F.col("lateness_min") <= LATE_BOUND)).cast("int"))
     .withColumn("is_late",  (F.col("lateness_min") > LATE_BOUND).cast("int"))
     .withColumn("is_early", (F.col("lateness_min") < EARLY_BOUND).cast("int"))
     .withColumn("hour", (F.floor(F.col("actual_secs_adj") / 3600) % 24).cast("int")))

def otp_agg(df, dims):
    return (df.groupBy(*dims)
        .agg(F.count("*").alias("observations"),
             F.sum("on_time").alias("on_time_n"),
             F.sum("is_late").alias("late_n"),
             F.sum("is_early").alias("early_n"),
             F.round(100.0 * F.sum("on_time") / F.count("*"), 1).alias("otp_pct"),
             F.round(F.avg("lateness_min"), 1).alias("avg_late_min"),
             F.round(F.expr("percentile(lateness_min, 0.5)"), 1).alias("median_late_min")))

# COMMAND ----------
otp_agg(L, ["route_id", "route_short_name", "route_long_name"]).orderBy("otp_pct") \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_route")

otp_agg(L, ["route_id", "route_short_name", "hour"]).orderBy("route_id", "hour") \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_route_hour")

# worst stops = where lateness concentrates (require min observations to be meaningful)
otp_agg(L, ["stop_id", "stop_name"]).filter(F.col("observations") >= 20).orderBy("otp_pct") \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_stop")

# COMMAND ----------
# MAGIC %md ## DQ gates
def dq(check, ok):
    print(("OK  " if ok else "FAIL"), check); assert ok, f"DQ FAILED: {check}"

r = spark.table("mbta.gold.otp_by_route")
dq("otp_by_route non-empty", r.count() > 0)
dq("otp_pct within 0..100", r.filter((F.col("otp_pct") < 0) | (F.col("otp_pct") > 100)).count() == 0)
dq("counts reconcile", r.filter(F.col("on_time_n") + F.col("late_n") + F.col("early_n") != F.col("observations")).count() == 0)
display(r.limit(20))

# COMMAND ----------
# Return a compact run summary (system OTP + worst routes/stops) for headless read-back.
import json
tot = L.count()
ot = L.agg(F.sum("on_time").alias("n")).first()["n"]
worst_routes = spark.table("mbta.gold.otp_by_route").orderBy("otp_pct").limit(8).collect()
worst_stops = spark.table("mbta.gold.otp_by_stop").orderBy("otp_pct").limit(8).collect()
summary = {
    "bounds": {"early": EARLY_BOUND, "late": LATE_BOUND},
    "observations": int(tot),
    "system_otp_pct": (round(100.0 * ot / tot, 1) if tot else None),
    "worst_routes": [{"route": (x["route_short_name"] or x["route_long_name"] or "?"),
                      "otp_pct": x["otp_pct"], "obs": int(x["observations"])} for x in worst_routes],
    "worst_stops": [{"stop": x["stop_name"], "otp_pct": x["otp_pct"], "obs": int(x["observations"])}
                    for x in worst_stops],
}
dbutils.notebook.exit(json.dumps(summary))
