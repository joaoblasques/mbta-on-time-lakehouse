# Databricks notebook source
# MAGIC %md
# MAGIC # Verify DLT ≡ Jobs marts
# MAGIC Asserts each `otp_by_*_dlt` materialized view equals its Jobs `gold.otp_by_*` table exactly
# MAGIC (same row count + symmetric `EXCEPT` empty). Run after a DLT pipeline update. Headless via
# MAGIC `databricks jobs submit`. Both live in `mbta.gold`.

# COMMAND ----------
import json

PAIRS = [("otp_by_route", "otp_by_route_dlt"),
         ("otp_by_route_hour", "otp_by_route_hour_dlt"),
         ("otp_by_stop", "otp_by_stop_dlt")]

results = {}
for jobs_name, dlt_name in PAIRS:
    jobs = spark.table(f"mbta.gold.{jobs_name}")
    dlt_tbl = spark.table(f"mbta.gold.{dlt_name}")
    n_jobs, n_dlt = jobs.count(), dlt_tbl.count()
    only_jobs = jobs.exceptAll(dlt_tbl).count()
    only_dlt = dlt_tbl.exceptAll(jobs).count()
    ok = (n_jobs == n_dlt) and only_jobs == 0 and only_dlt == 0
    results[jobs_name] = {"jobs": n_jobs, "dlt": n_dlt, "only_jobs": only_jobs,
                          "only_dlt": only_dlt, "equal": ok}
    print(("OK  " if ok else "FAIL"), jobs_name, results[jobs_name])
    assert ok, f"DLT != Jobs for {jobs_name}: {results[jobs_name]}"

dbutils.notebook.exit(json.dumps(results))
