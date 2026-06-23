# Databricks notebook source
# MAGIC %md
# MAGIC # 03-stream — Bronze RT via Auto Loader (incremental, PoC)
# MAGIC Proof of concept for the streaming cutover (see `docs/streaming.md`). Reads `.pb` snapshots
# MAGIC with **Auto Loader** (`cloudFiles`, binaryFile) and a **checkpoint**, so each run ingests
# MAGIC ONLY new files; **appends** to `mbta.bronze.rt_trip_updates_stream`. Runs as a scheduled
# MAGIC batch via `Trigger.AvailableNow` (no always-on cluster needed). Writes to a `_stream` table
# MAGIC so the live batch pipeline is untouched while we validate.

# COMMAND ----------
# MAGIC %pip install gtfs-realtime-bindings
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, IntegerType, LongType, StructField, StructType, StringType)

VOL = "/Volumes/mbta/bronze/rt_raw"
CKPT = "/Volumes/mbta/bronze/rt_raw/_ckpt_stream"   # checkpoint + schema bookmark live on the Volume
TABLE = "mbta.bronze.rt_trip_updates_stream"

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


# COMMAND ----------
src = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "binaryFile")
       .option("cloudFiles.schemaLocation", f"{CKPT}/trip_updates/_schema")
       .option("cloudFiles.maxFilesPerTrigger", "128")  # bounded micro-batches under AvailableNow
       .option("pathGlobFilter", "*.pb")
       .option("recursiveFileLookup", "true")
       .load(f"{VOL}/trip_updates"))

# repartition so each task parses ~1 file → the protobuf explode stays within tiny serverless workers
parsed = (src.repartition(128)
          .select(F.explode(parse_trip_updates("path", "content")).alias("r"))
          .select("r.*")
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("arrival_ts", F.col("arrival_time").cast("timestamp")))

q = (parsed.writeStream
     .option("checkpointLocation", f"{CKPT}/trip_updates/_chk")
     .trigger(availableNow=True)
     .toTable(TABLE))
q.awaitTermination()

# COMMAND ----------
n = spark.table(TABLE).count()
files = spark.table(TABLE).select("_source_file").distinct().count()
print(f"{TABLE}: rows={n}, distinct source files ingested={files}")
