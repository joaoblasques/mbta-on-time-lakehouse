# Architecture & Hard Decisions

End-to-end design plus the decisions that shaped it. The decisions are the interview ammo —
each is *why this, given the constraints*, not just *what*.

## Medallion flow

- **Bronze — raw, as-ingested.** GTFS **static** (routes/stops/trips/stop_times) lands in a
  Databricks managed Volume → Delta; GTFS-**Realtime** `.pb` snapshots land in GCS via the
  scheduled poller, then are copied into a Volume and decoded to Delta
  (`rt_trip_updates`, `rt_vehicle_positions`). Kept faithful (raw strings / raw bytes) + provenance.
- **Silver — cleaned facts.** Typed with explicit schemas, deduped, joined; `trip_stop_lateness`
  computes minutes-late per (trip, stop). Granular and **policy-neutral**.
- **Gold — business marts.** `otp_by_route` / `otp_by_route_hour` / `otp_by_stop`: OTP %,
  with the **"on-time" policy applied here** (not in silver). Serves the AI/BI dashboard.

---

## Hard decisions

### 1. Cloud: AWS → GCP (mid-build pivot)
**Decision:** run on **GCP + Databricks** instead of the originally-planned AWS + Databricks.
**Why:** the AWS account was inaccessible (root email had no account; legacy IAM creds failed;
new signup blocked at CAPTCHA). GCP signs in with the existing Google account — zero friction —
and ships $300/90-day credits. **Tradeoff:** rewrote IaC + ingestion for GCS/Pub-Sub; documented
the pivot rather than hiding it (decisions should survive reality).

### 2. Databricks Free Edition + the cross-cloud read boundary  *(Probe-then-Fallback)*
**Decision:** use **Databricks Free Edition** ($0) and ingest RT by **copying GCS → a managed
Volume**, rather than reading GCS in place via a Unity Catalog external location.
**Why:** I tested the elegant path with a *single* command — `databricks storage-credentials create`
— which failed: **Workload Identity Federation is not enabled** in Free Edition, so it can't mint a
cross-cloud GCP identity. One command gave a definitive answer; I fell back to the always-allowed
managed-Volume path. **Tradeoff:** the fallback *copies* bytes (less elegant, duplicate storage)
and the GCS→Databricks hop isn't automatic. In a paid workspace you'd wire a storage credential +
external location and read GCS directly. **Lesson:** know the platform boundary cheaply, keep a
fallback ready. (See [design-patterns.md](design-patterns.md).)

### 3. Scheduled ingestion: Cloud Run **Job** + Cloud Scheduler
**Decision:** containerize the poller and run it as a **Cloud Run Job** fired by **Cloud Scheduler**
every 2 min, all in Terraform.
**Why:** a Job (run-to-completion) fits a batch poll better than an always-on service; Scheduler
gives a cron without a VM. A **least-privilege** runtime SA (objectAdmin on the one bucket) and a
separate scheduler SA (`run.invoker`) keep blast radius small. **Tradeoff:** a 2-min cadence runs
indefinitely (trivial cost, within credits); pause with `gcloud scheduler jobs pause`.

### 4. Binary feed ingestion: `binaryFile` → protobuf decode → Delta
**Decision:** read `.pb` as raw bytes (`spark.read.format("binaryFile")`), decode each
`FeedMessage` with `gtfs_realtime_pb2`, write explicit-schema Delta.
**Why:** Spark can't parse protobuf natively. **Scale knob:** parse on the **driver** at this
small volume (`collect`), swap to a UDF / `mapInPandas` when data grows — same logic, distributed.
Knowing when the simple way suffices is the senior call.

### 5. Where business rules live: silver vs gold
**Decision:** silver stores neutral facts (`lateness_min`); the **"on-time" definition (−1..+5 min)
lives in gold**.
**Why:** changing "on-time = within 3 min" then touches **one gold notebook** — silver and the
expensive lateness join don't move — and other gold marts (avg delay, reliability index) can derive
from the same silver without re-cleaning. Keep policy out of the granular layer.

### 6. The lateness time reconciliation (the genuinely tricky bit)
**Decision:** convert RT actual arrival (**epoch, UTC**) to **local service-day seconds**
(`from_utc_timestamp` → America/New_York → seconds-after-midnight), then subtract the schedule's
`arrival_secs`, with an **after-midnight wrap correction** for GTFS times ≥ 24:00:00.
**Why:** RT is an absolute instant; the schedule is a local time-of-day that can exceed 24h
(after-midnight service). Naively subtracting gives garbage across the midnight boundary.

### 7. Dedup RT to the latest prediction per (trip, stop)
**Decision:** `row_number()` over `(trip_id, stop_id)` ordered by `feed_ts` desc, keep #1.
**Why:** the feed re-states each stop across many snapshots; the latest is the most refined.
**Honest caveat:** `trip_update.arrival_time` is often a *prediction*; over a short window it's an
actual-*proxy*. Real OTP confirms against observed arrival over a full day — which is why the
scheduled poller (decision #3) exists.

### 8. Explicit schemas + DQ asserts, everywhere
**Decision:** define `StructType` schemas (no inference for the contract); gate every notebook with
`assert`s (non-empty, key uniqueness, referential integrity, OTP ∈ [0,100], count reconciliation).
**Why:** a coding agent once silently loaded a fraction of rows and dropped columns — accuracy here
is a *context + verification* problem. Asserts turn silent-wrong-data into a loud failed job.

### 9. Reproducibility: Terraform remote state in GCS, mise-pinned tools
**Decision:** Terraform state in a versioned GCS bucket; tool versions pinned in `mise.toml`;
deps in `uv`. **Why:** anyone (or CI) can reproduce the exact toolchain + infra from the repo.

---

## Known limitations (honest)
- OTP numbers reflect a short capture window until the poller accumulates days of history.
- Free Edition: no direct GCS read (decision #2), one metastore, restricted networking.
- Liquid Clustering is the intended layout for scale; not yet applied at current data volume.
- True streaming (Pub/Sub → Structured Streaming) and CI are provisioned/planned, not yet built.
