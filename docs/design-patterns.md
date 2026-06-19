# Design Patterns — MBTA On-Time Lakehouse

Reusable patterns discovered while building this project. Each is written to be lifted into
*other* DE projects, not just this one. See also the concepts doc and
the README.

---

## Pattern 1 — Probe-then-Fallback (managed-platform capability limits)

**When to use:** you're building on a managed platform (e.g. Databricks Free Edition, any
SaaS tier) whose feature limits you don't know up front.

**Problem:** you can't tell if a capability is available without trying it — and wiring up the
*whole* chain just to discover it's blocked wastes hours.

**Solution:** run the **single cheapest command** that exercises the capability, read the
pass/fail, and have a **known fallback ready before you start**.

**This project's instance:**
- Probe: `databricks storage-credentials create … databricks_gcp_service_account:{}`
- Result: ❌ *Workload Identity Federation not enabled* — Free Edition can't mint a
  cross-cloud GCP identity for a Unity Catalog storage credential.
- Fallback: copy the GCS objects into a Databricks **managed Volume** (`mbta.bronze.rt_raw`).
  The platform *always* lets you read its own managed storage.

**Consequences:** minimal time sunk; the boundary becomes documented knowledge; the fallback
is slightly less elegant (it *copies* bytes instead of reading in place) but robust.

**Generalizes to:** any "does this tier allow X?" — test with the smallest invocation, fall
back to the always-allowed path, write down the boundary you found.

---

## Pattern 2 — Raw-Bytes → Decode → Structured Delta (binary-feed ingestion)

**When to use:** ingesting a non-tabular **binary** feed (protobuf, custom formats) Spark
can't parse natively.

**Problem:** the bronze source is opaque bytes; Spark readers expect CSV/JSON/Parquet.

**Solution:**
1. Read raw bytes with `spark.read.format("binaryFile")` (one row/file: `path`, `length`, `content`).
2. **Decode** with the format's own library (here `gtfs_realtime_pb2`), exploding nested
   records into rows.
3. Apply an **explicit schema** (never inference for the contract) + ingestion metadata
   (`_source_file`, `_ingested_at`).
4. Write **idempotent** Delta (`overwrite` for a full reload).
5. **Scale knob:** parse on the **driver** (`collect`) at small volume; swap to a
   **UDF / `mapInPandas`** when data grows — *same decode logic, distributed*. Knowing when
   the simple way suffices is the senior call.
6. **Verify, don't trust:** DQ `assert`s (non-empty, has-real-values) so a bad parse *fails
   the job* instead of silently landing garbage.

**Consequences:** faithful bronze, a testable parse (pure decode separated from I/O), and a
clear scale path that doesn't require a rewrite.

**Generalizes to:** any binary/semi-structured source landed as files then decoded — IoT
protobuf, Avro-without-registry, vendor blobs.

---

## The combined flow (this project's realtime path)

```
GCS .pb (cross-cloud read BLOCKED)
   └─[Pattern 1: probe → fallback]→ managed Volume  mbta.bronze.rt_raw
          └─[Pattern 2: binaryFile → decode → Delta]→ mbta.bronze.rt_trip_updates / rt_vehicle_positions
                 └─ join to silver stop_times (the schedule) → LATENESS
```

**North-star note:** these are general DE patterns — strong candidates to file into the
**Corpus** (interactive session) so they compound beyond this one project.
