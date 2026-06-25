# Databricks notebook source
# MAGIC %md
# MAGIC # DLT — OTP marts (Lakeflow Declarative Pipelines, productionized)
# MAGIC Declares the three gold OTP marts as materialized views over the Jobs-produced
# MAGIC `mbta.silver.trip_stop_lateness`. The OTP logic is **imported from the tested wheel**
# MAGIC (`transforms.otp` — the same functions `05_gold_otp.py` calls), so the DLT marts equal the
# MAGIC Jobs marts by construction. Data quality is declarative via `@dlt.expect`. Runs serverless,
# MAGIC on-demand (€0 idle). See `docs/lakeflow-dlt.md` + arch decision #18.

# COMMAND ----------
import dlt

from transforms.otp import by_route, by_route_hour, by_stop  # tested wheel (attached by the bundle)

SILVER = "mbta.silver.trip_stop_lateness"


@dlt.table(name="otp_by_route_dlt", comment="OTP per route (declarative). Mirrors gold.otp_by_route.")
@dlt.expect("otp_pct_in_range", "otp_pct BETWEEN 0 AND 100")
@dlt.expect("counts_reconcile", "on_time_n + late_n + early_n = observations")
@dlt.expect_or_drop("has_observations", "observations > 0")
def otp_by_route_dlt():
    return by_route(spark.read.table(SILVER))


@dlt.table(name="otp_by_route_hour_dlt",
           comment="OTP per route × hour (declarative). Mirrors gold.otp_by_route_hour.")
@dlt.expect("otp_pct_in_range", "otp_pct BETWEEN 0 AND 100")
@dlt.expect("counts_reconcile", "on_time_n + late_n + early_n = observations")
@dlt.expect_or_drop("has_observations", "observations > 0")
def otp_by_route_hour_dlt():
    return by_route_hour(spark.read.table(SILVER))


@dlt.table(name="otp_by_stop_dlt", comment="OTP per stop, ≥20 obs (declarative). Mirrors gold.otp_by_stop.")
@dlt.expect("otp_pct_in_range", "otp_pct BETWEEN 0 AND 100")
@dlt.expect("counts_reconcile", "on_time_n + late_n + early_n = observations")
@dlt.expect_or_drop("has_observations", "observations > 0")
def otp_by_stop_dlt():
    return by_stop(spark.read.table(SILVER))
