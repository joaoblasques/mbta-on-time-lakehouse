# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Bronze RT: decode GTFS-Realtime protobuf → Delta
# MAGIC Reads raw `.pb` snapshots from the `mbta.bronze.rt_raw` Volume, decodes each
# MAGIC `FeedMessage` (`gtfs_realtime_pb2`), and writes structured bronze tables:
# MAGIC - `mbta.bronze.rt_trip_updates` — actual/predicted arrival times (drives lateness)
# MAGIC - `mbta.bronze.rt_vehicle_positions` — where each vehicle was
# MAGIC Idempotent (overwrite). Driver-side parse (fine at this volume; for scale → a UDF/mapInPandas).

# COMMAND ----------
# MAGIC %pip install gtfs-realtime-bindings
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from google.transit import gtfs_realtime_pb2
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               IntegerType, DoubleType)

VOL = "/Volumes/mbta/bronze/rt_raw"

def read_pb_files(feed: str):
    """(source_file, content_bytes) for one feed's .pb snapshots. Explicit, small-volume collect."""
    df = (spark.read.format("binaryFile")
          .option("recursiveFileLookup", "true").option("pathGlobFilter", "*.pb")
          .load(f"{VOL}/{feed}"))
    return [(r["path"].split("/")[-1], r["content"]) for r in df.collect()]

# COMMAND ----------
# --- trip_updates: the lateness driver (arrival/departure per stop) ---
TU_SCHEMA = StructType([
    StructField("feed_ts", LongType()),     StructField("trip_id", StringType()),
    StructField("route_id", StringType()),  StructField("vehicle_id", StringType()),
    StructField("stop_id", StringType()),   StructField("stop_sequence", IntegerType()),
    StructField("arrival_time", LongType()), StructField("departure_time", LongType()),
    StructField("schedule_relationship", IntegerType()), StructField("_source_file", StringType()),
])

def parse_trip_updates(files):
    rows = []
    for src, content in files:
        fm = gtfs_realtime_pb2.FeedMessage(); fm.ParseFromString(content)
        feed_ts = int(fm.header.timestamp)
        for e in fm.entity:
            if not e.HasField("trip_update"):
                continue
            tu = e.trip_update
            for s in tu.stop_time_update:
                rows.append((
                    feed_ts, tu.trip.trip_id or None, tu.trip.route_id or None,
                    tu.vehicle.id or None, s.stop_id or None,
                    int(s.stop_sequence) if s.HasField("stop_sequence") else None,
                    int(s.arrival.time) if s.HasField("arrival") and s.arrival.time else None,
                    int(s.departure.time) if s.HasField("departure") and s.departure.time else None,
                    int(s.schedule_relationship), src,
                ))
    return rows

tu = (spark.createDataFrame(parse_trip_updates(read_pb_files("trip_updates")), TU_SCHEMA)
      .withColumn("_ingested_at", F.current_timestamp())
      .withColumn("arrival_ts", F.col("arrival_time").cast("timestamp")))  # epoch s -> ts (UTC)
tu.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.bronze.rt_trip_updates")
print("rt_trip_updates rows:", spark.table("mbta.bronze.rt_trip_updates").count())

# COMMAND ----------
# --- vehicle_positions: where each vehicle was ---
STATUS = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}
VP_SCHEMA = StructType([
    StructField("trip_id", StringType()),  StructField("route_id", StringType()),
    StructField("vehicle_id", StringType()), StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),      StructField("current_status", StringType()),
    StructField("stop_id", StringType()),  StructField("current_stop_sequence", IntegerType()),
    StructField("vehicle_ts", LongType()), StructField("_source_file", StringType()),
])

def parse_vehicle_positions(files):
    rows = []
    for src, content in files:
        fm = gtfs_realtime_pb2.FeedMessage(); fm.ParseFromString(content)
        for e in fm.entity:
            if not e.HasField("vehicle"):
                continue
            v = e.vehicle
            rows.append((
                v.trip.trip_id or None, v.trip.route_id or None, v.vehicle.id or None,
                float(v.position.latitude) if v.HasField("position") else None,
                float(v.position.longitude) if v.HasField("position") else None,
                STATUS.get(v.current_status, str(v.current_status)),
                v.stop_id or None,
                int(v.current_stop_sequence) if v.HasField("current_stop_sequence") else None,
                int(v.timestamp) if v.HasField("timestamp") else None, src,
            ))
    return rows

vp = (spark.createDataFrame(parse_vehicle_positions(read_pb_files("vehicle_positions")), VP_SCHEMA)
      .withColumn("_ingested_at", F.current_timestamp())
      .withColumn("vehicle_ts_utc", F.col("vehicle_ts").cast("timestamp")))
vp.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.bronze.rt_vehicle_positions")
print("rt_vehicle_positions rows:", spark.table("mbta.bronze.rt_vehicle_positions").count())

# COMMAND ----------
# MAGIC %md ## DQ gates + peek
def dq(check, ok):
    print(("OK " if ok else "FAIL"), check)
    assert ok, f"DQ FAILED: {check}"

t = spark.table("mbta.bronze.rt_trip_updates")
p = spark.table("mbta.bronze.rt_vehicle_positions")
dq("trip_updates non-empty", t.count() > 0)
dq("vehicle_positions non-empty", p.count() > 0)
dq("trip_updates has real arrivals", t.filter(F.col("arrival_time").isNotNull()).count() > 0)
display(t.orderBy("trip_id", "stop_sequence").limit(10))
display(p.limit(10))
