# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: typed, cleaned, conformed GTFS schedule
# MAGIC Bronze raw-strings → explicit types + selected columns. Parses GTFS times
# MAGIC (which can exceed 24:00:00 for after-midnight service) into seconds-after-midnight.
# MAGIC Runs DQ gates before exposing data. Idempotent (overwrite).

# COMMAND ----------
from pyspark.sql import functions as F
SRC, DST = "mbta.bronze", "mbta.silver"

def gtfs_secs(col):
    """GTFS HH:MM:SS (hour may be >=24) -> int seconds-after-midnight; null if blank."""
    p = F.split(F.col(col), ":")
    return p.getItem(0).cast("int")*3600 + p.getItem(1).cast("int")*60 + p.getItem(2).cast("int")

# COMMAND ----------
(spark.table(f"{SRC}.routes").select(
    F.trim("route_id").alias("route_id"),
    F.trim("route_short_name").alias("route_short_name"),
    F.trim("route_long_name").alias("route_long_name"),
    F.col("route_type").cast("int").alias("route_type"),
 ).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{DST}.routes"))

# COMMAND ----------
(spark.table(f"{SRC}.stops").select(
    F.trim("stop_id").alias("stop_id"),
    F.trim("stop_name").alias("stop_name"),
    F.col("stop_lat").cast("double").alias("stop_lat"),
    F.col("stop_lon").cast("double").alias("stop_lon"),
    F.trim("parent_station").alias("parent_station"),
 ).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{DST}.stops"))

# COMMAND ----------
(spark.table(f"{SRC}.trips").select(
    F.trim("trip_id").alias("trip_id"),
    F.trim("route_id").alias("route_id"),
    F.trim("service_id").alias("service_id"),
    F.col("direction_id").cast("int").alias("direction_id"),
    F.trim("trip_headsign").alias("trip_headsign"),
 ).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{DST}.trips"))

# COMMAND ----------
(spark.table(f"{SRC}.stop_times").select(
    F.trim("trip_id").alias("trip_id"),
    F.trim("stop_id").alias("stop_id"),
    F.col("stop_sequence").cast("int").alias("stop_sequence"),
    F.trim("arrival_time").alias("arrival_time"),
    gtfs_secs("arrival_time").alias("arrival_secs"),
    gtfs_secs("departure_time").alias("departure_secs"),
 ).filter(F.col("trip_id").isNotNull() & F.col("stop_id").isNotNull())
  .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{DST}.stop_times"))

# COMMAND ----------
# MAGIC %md ## DQ gates — fail loudly before exposing data
def dq(check, ok):
    print(("✅" if ok else "❌"), check); assert ok, f"DQ FAILED: {check}"

r,s,t,st = (spark.table(f"{DST}.{x}") for x in ["routes","stops","trips","stop_times"])
dq("routes.route_id unique", r.count()==r.select("route_id").distinct().count())
dq("stops.stop_id unique",   s.count()==s.select("stop_id").distinct().count())
dq("trips.trip_id unique",   t.count()==t.select("trip_id").distinct().count())
dq("stop_times keys non-null", st.filter(F.col("trip_id").isNull()|F.col("stop_id").isNull()).count()==0)
dq("stop_times.trip_id ⊆ trips", st.join(t,"trip_id","left_anti").count()==0)

# COMMAND ----------
for x in ["routes","stops","trips","stop_times"]:
    print(x, spark.table(f"{DST}.{x}").count())
display(spark.table("mbta.silver.stop_times").limit(10))
