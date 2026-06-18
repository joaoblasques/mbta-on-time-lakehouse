# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: MBTA GTFS static schedules
# MAGIC Reads raw GTFS CSVs from the `mbta.bronze.raw` volume → Delta tables in `mbta.bronze`.
# MAGIC **Idempotent** (overwrite). Bronze keeps faithful raw strings + ingestion metadata;
# MAGIC typing/cleaning happens in silver.

# COMMAND ----------
from pyspark.sql import functions as F

RAW = "/Volumes/mbta/bronze/raw"
CATALOG, SCHEMA = "mbta", "bronze"
FILES = {"routes": "routes.csv", "stops": "stops.csv", "trips": "trips.csv"}

# COMMAND ----------
def ingest_bronze(table: str, filename: str) -> int:
    df = (
        spark.read.option("header", True).option("inferSchema", False)  # raw fidelity
        .csv(f"{RAW}/{filename}")
        .withColumn("_ingested_at", F.current_timestamp())   # provenance
        .withColumn("_source_file", F.lit(filename))
    )
    target = f"{CATALOG}.{SCHEMA}.{table}"
    (df.write.mode("overwrite").option("overwriteSchema", True)  # idempotent re-run
       .saveAsTable(target))
    n = spark.table(target).count()
    print(f"{target}: {n} rows")
    return n

# COMMAND ----------
for tbl, fname in FILES.items():
    ingest_bronze(tbl, fname)

# COMMAND ----------
# MAGIC %md ## Verify
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
display(spark.table("mbta.bronze.routes").limit(10))
