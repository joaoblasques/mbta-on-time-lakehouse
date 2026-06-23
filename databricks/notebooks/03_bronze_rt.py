# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Bronze RT: decode GTFS-Realtime protobuf → Delta
# MAGIC Reads raw `.pb` snapshots from the `mbta.bronze.rt_raw` Volume, decodes each
# MAGIC `FeedMessage` (`gtfs_realtime_pb2`), and writes structured bronze tables:
# MAGIC - `mbta.bronze.rt_trip_updates` — actual/predicted arrival times (drives lateness)
# MAGIC - `mbta.bronze.rt_vehicle_positions` — where each vehicle was
# MAGIC
# MAGIC **Scales:** the protobuf parse runs **distributed** (a UDF on executors — no driver
# MAGIC `collect()`), over a **bounded rolling window** (`WINDOW_DAYS`, via binaryFile
# MAGIC `modifiedAfter`). This keeps memory bounded as history grows — OTP is a recent-performance
# MAGIC metric, so a rolling window is the right semantic. (True append-incremental via Auto
# MAGIC Loader is the streaming Phase-2 item.) Idempotent overwrite.

# COMMAND ----------
# MAGIC %pip install gtfs-realtime-bindings
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, LongType,
                               StructField, StructType, StringType)

VOL = "/Volumes/mbta/bronze/rt_raw"
WINDOW_DAYS = 2          # rolling window of recent service days (bounds compute on tiny serverless)
NPART = 128              # spread files across many tasks → low per-worker memory in the parse UDF


def read_pb(feed: str):
    """DataFrame[path, content] of the last WINDOW_DAYS `dt=` partitions only.

    Bounds by the data's *date partition* (not file mtime — the recovered backlog was just
    re-copied, so mtime is unreliable), and repartitions so each Python worker parses few files.
    """
    base = f"{VOL}/{feed}"
    wanted = {(datetime.now(timezone.utc) - timedelta(days=i)).strftime("dt=%Y-%m-%d")
              for i in range(WINDOW_DAYS)}
    try:
        paths = [f.path for f in dbutils.fs.ls(base)
                 if f.path.rstrip("/").split("/")[-1] in wanted]
    except Exception:
        paths = []
    if not paths:
        return spark.createDataFrame([], "path string, content binary")
    return (spark.read.format("binaryFile")
            .option("recursiveFileLookup", "true").option("pathGlobFilter", "*.pb")
            .load(paths)  # list of partition dirs (NOT *paths — 2nd positional = format)
            .repartition(NPART)
            .select("path", "content"))


# COMMAND ----------
# --- trip_updates: the lateness driver (arrival/departure per stop) ---
TU_ROW = StructType([
    StructField("feed_ts", LongType()),     StructField("trip_id", StringType()),
    StructField("route_id", StringType()),  StructField("vehicle_id", StringType()),
    StructField("stop_id", StringType()),   StructField("stop_sequence", IntegerType()),
    StructField("arrival_time", LongType()), StructField("departure_time", LongType()),
    StructField("schedule_relationship", IntegerType()), StructField("_source_file", StringType()),
])


@F.udf(ArrayType(TU_ROW))
def parse_trip_updates(path, content):
    """bytes → list of trip_update rows. Runs on executors (distributed), so no driver OOM."""
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


tu = (read_pb("trip_updates")
      .select(F.explode(parse_trip_updates("path", "content")).alias("r"))
      .select("r.*")
      .withColumn("_ingested_at", F.current_timestamp())
      .withColumn("arrival_ts", F.col("arrival_time").cast("timestamp")))  # epoch s -> ts (UTC)
tu.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.bronze.rt_trip_updates")
print("rt_trip_updates rows:", spark.table("mbta.bronze.rt_trip_updates").count())

# COMMAND ----------
# --- vehicle_positions: where each vehicle was ---
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


vp = (read_pb("vehicle_positions")
      .select(F.explode(parse_vehicle_positions("path", "content")).alias("r"))
      .select("r.*")
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
