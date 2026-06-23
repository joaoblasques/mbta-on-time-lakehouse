# Databricks notebook source
# MAGIC %md
# MAGIC # DLT spike — OTP by route, declared (Lakeflow Declarative Pipelines)
# MAGIC A minimal **declarative** slice: one `@dlt.table` reading existing silver, with a built-in
# MAGIC data-quality **expectation**. DLT (not regular notebooks) manages the dependency graph,
# MAGIC incremental execution, retries, and DQ. `import dlt` only resolves inside a DLT pipeline.
# MAGIC Spike goal: does a serverless DLT pipeline run on Databricks Free Edition? See `docs/streaming.md`.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

EARLY, LATE = -1.0, 5.0


@dlt.table(
    name="otp_by_route_dlt",
    comment="OTP per route, declared via DLT (spike). Mirrors gold.otp_by_route.",
)
@dlt.expect("otp_pct_in_range", "otp_pct BETWEEN 0 AND 100")          # warn-only DQ expectation
@dlt.expect_or_drop("has_observations", "observations > 0")           # drop rows failing this
def otp_by_route_dlt():
    lateness = (spark.read.table("mbta.silver.trip_stop_lateness")
                .filter(F.col("lateness_min").isNotNull())
                .withColumn("on_time", ((F.col("lateness_min") >= EARLY)
                                        & (F.col("lateness_min") <= LATE)).cast("int")))
    return (lateness.groupBy("route_id", "route_short_name", "route_long_name")
            .agg(F.count("*").alias("observations"),
                 F.sum("on_time").alias("on_time_n"),
                 F.round(100.0 * F.sum("on_time") / F.count("*"), 1).alias("otp_pct")))
