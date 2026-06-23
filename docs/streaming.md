# Streaming / incremental ingestion (Phase 2)

## ✅ SHIPPED — the live pipeline is now incremental (2026-06-23)
The cutover is done. Production `03_bronze_rt` is **Structured Streaming + Auto Loader**
(`cloudFiles` binaryFile, `Trigger.AvailableNow`) for **both** feeds, **appending** to the real
`mbta.bronze.rt_*` tables — full history retained, only new files processed each run (exactly-once
via checkpoint, `maxFilesPerTrigger=128` + `repartition(128)`). `04_silver_lateness` now **windows**
bronze by recent `feed_ts` (3 days) so compute stays bounded as bronze grows. The batch-window
bronze (decision #12) is retired. Verified at cutover: bronze 32.6M rows / 1,657 files, silver
518k, gold 174 routes, system OTP 59.6%. The self-managing monitor watched the change.

## ✅ Proof of concept — VALIDATED (2026-06-23)
A streaming bronze (`databricks/notebooks/03_bronze_rt_stream.py`) using **Auto Loader**
(`cloudFiles`, binaryFile) + **`Trigger.AvailableNow`** runs cleanly on Free-Edition serverless and
is **proven incremental**: run 1 ingested 1,610 `.pb` files; run 2 ingested only **+7** (the new
files the live poller landed in between) — the checkpoint skips everything already processed.

**The gotcha + fix:** the protobuf parse explodes each file into thousands of rows in a Python UDF,
which **OOMs the tiny serverless worker** if a micro-batch is too big (same root cause as the batch
bronze). Fixed with **`cloudFiles.maxFilesPerTrigger=128`** (bounded micro-batches) **+
`.repartition(128)`** (≈1 file per task). Checkpoint + schema location on a **UC Volume** work fine.
Writes append to `mbta.bronze.rt_trip_updates_stream` (a `_stream` table — prod is untouched).

**Remaining = the cutover** (see Plan): swap prod `03` to this, point `04` at a windowed read,
retire the batch-window bronze. Done dev→prod with the monitor watching.

## Spike findings (2026-06-23, Free Edition)
- **No always-on cluster.** Free Edition runs serverless jobs only — there is no cheap 24/7
  cluster, so a *continuous* stream isn't the right fit. The idiomatic answer is **Structured
  Streaming with `Trigger.AvailableNow`**: a streaming query that processes all *new* data since
  the last checkpoint, then stops. Same streaming semantics + exactly-once checkpointing, but it
  runs as a scheduled batch — perfect for an hourly job.
- **Auto Loader (`cloudFiles`) is available** (ships with DBR / serverless). It tracks which files
  it has already ingested via a checkpoint, so each run reads only *new* `.pb` snapshots — proper
  incremental ingestion, replacing today's bounded-rolling-window hack.
- **Lakeflow Declarative Pipelines (DLT):** the pipelines API is reachable, but DLT on Free
  Edition is unverified at runtime (it wants pipeline compute). Treated as a **stretch**; the
  Jobs + Auto Loader path below is the reliable one and demonstrates the same skills.

## Why this matters
Today bronze re-reads a bounded window of the Volume every run (decision #12) — a deliberate cap
to survive tiny serverless memory. Auto Loader makes ingestion **truly incremental**: it processes
only new files, so bronze can retain **full history** without growing the per-run work. That's the
"streaming + batch unified on the same dataset" story, done within Free-Edition limits.

## Target design — incremental medallion
```
poller → GCS → copier → Volume(.pb)
                          │  Auto Loader (cloudFiles, binaryFile) + checkpoint
                          ▼  Trigger.AvailableNow  (new files only)
                    bronze_rt_* (APPEND, full history)        ← streaming ingestion
                          │  read recent service_date window (bounded compute)
                          ▼
                    silver.trip_stop_lateness  (overwrite, rolling window)
                          ▼
                    gold.otp_*  (overwrite)
```
- **Bronze (03):** `spark.readStream.format("cloudFiles").option("cloudFiles.format","binaryFile")`
  → the tested `transforms` UDF parse → `writeStream … .trigger(availableNow=True)
  .option("checkpointLocation", <Volume path>) … .toTable("mbta.bronze.rt_trip_updates")` (append).
  History retained; only new files parsed each run.
- **Silver (04):** filter bronze to the last *N* `service_date`s before computing lateness — keeps
  compute bounded even as bronze grows. (OTP is a recent-performance metric.)
- **Gold (05):** unchanged (reads the bounded silver).

## Why dev-first (and not a rushed prod cutover)
This rewires the live, self-managing pipeline's ingestion + storage semantics (append vs
overwrite, a new checkpoint, schema handling). The bronze OOM saga (decision #12) showed how an
ingestion change can cascade. So: build + run it in the bundle's **`dev`** target against
**separate `_stream` tables**, validate exactly-once + scaling, then cut the medallion over in a
single reviewed change and retire the windowed batch bronze.

## Plan
1. `transforms` already has the pure parse logic — reuse it in the streaming foreachBatch/UDF.
2. Dev: Auto Loader bronze → `mbta.bronze.rt_trip_updates_stream` (+ checkpoint on the Volume),
   `Trigger.AvailableNow`; verify it ingests only new files across two runs.
3. Dev: point a copy of silver at the `_stream` table with a `service_date` window; confirm OTP
   matches the batch path.
4. Cut over prod (bundle deploy): swap 03 to Auto Loader, 04 to windowed read; retire the batch
   window; keep the monitor watching.
5. Stretch: express the same DAG as a Lakeflow Declarative Pipeline if DLT runs on Free Edition.
