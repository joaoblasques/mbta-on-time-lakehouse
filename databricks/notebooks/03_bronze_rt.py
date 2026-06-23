# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Bronze RT: incremental ingestion via Auto Loader (Structured Streaming)
# MAGIC Reads raw `.pb` snapshots from the `mbta.bronze.rt_raw` Volume with **Auto Loader**
# MAGIC (`cloudFiles`, binaryFile) + a **checkpoint**, so each run ingests ONLY new files, decodes
# MAGIC the protobuf (`gtfs_realtime_pb2`), and **appends** to:
# MAGIC - `mbta.bronze.rt_trip_updates` — actual/predicted arrivals (drives lateness)
# MAGIC - `mbta.bronze.rt_vehicle_positions` — where each vehicle was
# MAGIC
# MAGIC Runs as a scheduled batch via `Trigger.AvailableNow` (no always-on cluster). **Full history**
# MAGIC is retained (append); silver windows it. The parse explodes each file into many rows, so we
# MAGIC bound each micro-batch (`maxFilesPerTrigger`) and `repartition` to ~1 file/task to fit the
# MAGIC tiny serverless workers. Exactly-once via the checkpoint. See `docs/streaming.md`.

# COMMAND ----------
# MAGIC %pip install gtfs-realtime-bindings
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, LongType,
                               StructField, StructType, StringType)

VOL = "/Volumes/mbta/bronze/rt_raw"
CKPT = "/Volumes/mbta/bronze/rt_raw/_ckpt"   # Auto Loader checkpoints + schema (per feed)
MAX_FILES = 128                               # bounded micro-batch under Trigger.AvailableNow
NPART = 128                                   # ~1 file/task → protobuf explode fits serverless

# COMMAND ----------
TU_ROW = StructType([
    StructField("feed_ts", LongType()),     StructField("trip_id", StringType()),
    StructField("route_id", StringType()),  StructField("vehicle_id", StringType()),
    StructField("stop_id", StringType()),   StructField("stop_sequence", IntegerType()),
    StructField("arrival_time", LongType()), StructField("departure_time", LongType()),
    StructField("schedule_relationship", IntegerType()), StructField("_source_file", StringType()),
])


@F.udf(ArrayType(TU_ROW))
def parse_trip_updates(path, content):
    from google.transit import gtfs_realtime_pb2
    fm = gtfs_realtime_pb2.FeedMessage()
    fm.ParseFromString(bytes(content))
    src = path.split("/")[-1]
    feed_ts = int(fm.header.timestamp)
    out = []
    for e in fm.entity:
        if not e.HasField("trip_update"):
            continue
        tu = e.trip_update
        for s in tu.stop_time_update:
            out.append((
                feed_ts, tu.trip.trip_id or None, tu.trip.route_id or None,
                tu.vehicle.id or None, s.stop_id or None,
                int(s.stop_sequence) if s.HasField("stop_sequence") else None,
                int(s.arrival.time) if s.HasField("arrival") and s.arrival.time else None,
                int(s.departure.time) if s.HasField("departure") and s.departure.time else None,
                int(s.schedule_relationship), src,
            ))
    return out


STATUS = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}
VP_ROW = StructType([
    StructField("trip_id", StringType()),  StructField("route_id", StringType()),
    StructField("vehicle_id", StringType()), StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),      StructField("current_status", StringType()),
    StructField("stop_id", StringType()),  StructField("current_stop_sequence", IntegerType()),
    StructField("vehicle_ts", LongType()), StructField("_source_file", StringType()),
])


@F.udf(ArrayType(VP_ROW))
def parse_vehicle_positions(path, content):
    from google.transit import gtfs_realtime_pb2
    fm = gtfs_realtime_pb2.FeedMessage()
    fm.ParseFromString(bytes(content))
    src = path.split("/")[-1]
    out = []
    for e in fm.entity:
        if not e.HasField("vehicle"):
            continue
        v = e.vehicle
        out.append((
            v.trip.trip_id or None, v.trip.route_id or None, v.vehicle.id or None,
            float(v.position.latitude) if v.HasField("position") else None,
            float(v.position.longitude) if v.HasField("position") else None,
            STATUS.get(v.current_status, str(v.current_status)),
            v.stop_id or None,
            int(v.current_stop_sequence) if v.HasField("current_stop_sequence") else None,
            int(v.timestamp) if v.HasField("timestamp") else None, src,
        ))
    return out


# COMMAND ----------
def ingest(feed, parse_udf, table, post):
    """Auto Loader incremental ingest of one feed → append to `table` (exactly-once via checkpoint)."""
    src = (spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "binaryFile")
           .option("cloudFiles.schemaLocation", f"{CKPT}/{feed}/_schema")
           .option("cloudFiles.maxFilesPerTrigger", str(MAX_FILES))
           .option("pathGlobFilter", "*.pb")
           .option("recursiveFileLookup", "true")
           .load(f"{VOL}/{feed}"))
    parsed = post(src.repartition(NPART)
                  .select(F.explode(parse_udf("path", "content")).alias("r"))
                  .select("r.*")
                  .withColumn("_ingested_at", F.current_timestamp()))
    (parsed.writeStream
     .option("checkpointLocation", f"{CKPT}/{feed}/_chk")
     .trigger(availableNow=True)
     .toTable(table)).awaitTermination()


ingest("trip_updates", parse_trip_updates, "mbta.bronze.rt_trip_updates",
       lambda df: df.withColumn("arrival_ts", F.col("arrival_time").cast("timestamp")))
ingest("vehicle_positions", parse_vehicle_positions, "mbta.bronze.rt_vehicle_positions",
       lambda df: df.withColumn("vehicle_ts_utc", F.col("vehicle_ts").cast("timestamp")))

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
print("rt_trip_updates rows:", t.count(), "| rt_vehicle_positions rows:", p.count())
