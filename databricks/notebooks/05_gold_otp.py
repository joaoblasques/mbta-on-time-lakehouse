# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold: On-Time Performance (OTP) marts
# MAGIC From `silver.trip_stop_lateness` → the headline metric, **OTP %**, as three marts:
# MAGIC - `mbta.gold.otp_by_route` — OTP per route (the scorecard)
# MAGIC - `mbta.gold.otp_by_route_hour` — OTP per route × hour-of-day (when does it degrade?)
# MAGIC - `mbta.gold.otp_by_stop` — OTP per stop (**where delays concentrate**)
# MAGIC
# MAGIC **"On time" is a product decision** — the band lives in the tested wheel `transforms.otp`
# MAGIC (`classify` tags early/on-time/late + hour; `otp_agg` rolls up). This notebook is I/O +
# MAGIC DQ around it. See `docs/testing.md`. Idempotent (overwrite).

# COMMAND ----------
from pyspark.sql import functions as F

from transforms.otp import by_route, by_route_hour, by_stop, classify  # tested wheel (Asset Bundle)

spark.sql("CREATE SCHEMA IF NOT EXISTS mbta.gold")

# COMMAND ----------
RAW = spark.table("mbta.silver.trip_stop_lateness")           # raw lateness; builders classify internally

by_route(RAW) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_route")

by_route_hour(RAW) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_route_hour")

by_stop(RAW) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_stop")

L = classify(RAW)                                             # classified frame for the run summary below

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
# Compact run summary (system OTP + worst routes/stops) for headless read-back.
import json

tot = L.count()
ot = L.agg(F.sum("on_time").alias("n")).first()["n"]
worst_routes = spark.table("mbta.gold.otp_by_route").orderBy("otp_pct").limit(8).collect()
worst_stops = spark.table("mbta.gold.otp_by_stop").orderBy("otp_pct").limit(8).collect()
summary = {
    "observations": int(tot),
    "system_otp_pct": (round(100.0 * ot / tot, 1) if tot else None),
    "worst_routes": [{"route": (x["route_short_name"] or x["route_long_name"] or "?"),
                      "otp_pct": x["otp_pct"], "obs": int(x["observations"])} for x in worst_routes],
    "worst_stops": [{"stop": x["stop_name"], "otp_pct": x["otp_pct"], "obs": int(x["observations"])}
                    for x in worst_stops],
}
dbutils.notebook.exit(json.dumps(summary))
