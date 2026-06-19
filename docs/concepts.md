# Concepts Explained — MBTA On-Time Lakehouse

Plain-language glossary of the DE + transit concepts behind this project, tied to what
we're actually building. Written while learning — see also the README.

## The one equation everything serves

The project answers **"is the train late, and why?"** To know lateness, you subtract two things:

```
LATENESS  =  when the train ACTUALLY arrived   −   when it was SCHEDULED to arrive
             └─ "actuals" (from RT feed) ─┘        └─ from static GTFS stop_times ─┘
```

Every concept below is a piece of getting the left-hand side of that subtraction.

## The concepts

**RT (Real-Time)** — MBTA publishes two kinds of data: the **static schedule** (routes,
stops, trips, stop_times — *the plan*, changes ~weekly) and **GTFS-Realtime (RT)** — *what's
happening right now*, where every vehicle is this second, refreshed every few seconds (the
`.pb` files the poller fetches). RT is the only way to know what *actually* happened.

**Time series** — data points stamped through time. One RT snapshot is a single freeze-frame
("at 14:25:30, train 1234 was at Park St") — useless alone for lateness. Polling *repeatedly*
yields a *sequence over time*, so you can watch a train approach a stop and detect *when* it
arrived. That's why the poller runs on a loop.

**Actuals** — the standard DE phrase is **"actuals vs. planned."** The schedule is the plan;
RT gives the **actuals** (what really happened). On-time performance = actuals measured
against the plan. Our 63 `.pb` snapshots *are* the actuals.

**Lateness story** — shorthand for the end-to-end analytical arc: *raw RT → parse → match each
actual arrival to its scheduled arrival → compute minutes late → roll up into on-time-
performance % by route/stop/hour.* It's the bronze → silver → gold journey, ending in the
answer a transit manager cares about.

**21/feed** — MBTA RT comes as **3 separate feeds** (streams): `vehicle_positions` (where each
vehicle is), `trip_updates` (predicted/actual arrival times per stop — *this one drives
lateness*), and `alerts` (disruptions). We polled 20× in a loop + 1 earlier one-shot = **21**
captures of *each* feed. 21 × 3 feeds = **63** files.

**Date-partitioned** — snapshots land in per-day folders:
`vehicle_positions/dt=2026-06-19/vehicle_positions_20260619T142530Z.pb`. The `dt=2026-06-19/`
folder is a **partition**. A later query for "OTP on June 19" reads *only that folder* and
skips every other day — **partition pruning**, a core DE performance + cost technique. The
`dt=` style is the convention Spark/Hive recognize automatically.

**Idempotent names** — **idempotent** = run the same operation many times, same result as
running it once (no duplicates, no damage). Each filename embeds a unique timestamp
(`…_20260619T142530Z.pb`), so re-polling never overwrites a prior snapshot and never dupes the
same instant. This is the #1 "senior" signal from the corpus rubric: a pipeline safe to
re-run/backfill. (Bronze tables get the same property via `overwrite` mode.)

## How it chains (the medallion path)

```
RT feeds (3) ──poll on a loop──► time series of .pb snapshots in GCS   ← we are here (63 files)
   (actuals)                      (date-partitioned, idempotent names)
        │ parse protobuf → rows
        ▼
   bronze_rt (Delta)  ── join to static stop_times (the plan) ──►  silver: LATENESS
        │
        ▼
   gold: on-time-performance % by route / stop / hour   ← the "lateness story" payoff
```

## Cross-cloud storage access — UC credentials & external locations

How Databricks reads files that live in *your* GCS bucket (the "cross-cloud spike").

**Service account (SA)** — a *non-human* identity (a "robot account") software uses to
authenticate instead of a person. Own email-like ID, own permissions. Automation gets
exactly the access it needs.

**UC storage credential** — a Unity Catalog object holding a cloud identity (a GCP SA) that
Databricks "becomes" to reach external storage. `"databricks_gcp_service_account": {}` means
"Databricks, generate the SA for me" — it creates one and returns its email. *The key.*

**IAM (Identity & Access Management)** — the cloud permission system: **who** (a *member*:
user or SA) may do **what** (a *role*: a permission bundle) on **which** resource. A **policy
binding** attaches (member + role) to a resource. Granting the SA `roles/storage.objectViewer`
(read only — not write/admin) is **least privilege**, a senior signal.

**External location** — a UC object = a path (`gs://bucket`) + a storage credential. Declares
the path governed by UC, reached via that key; READ/WRITE grants live here. **Governance**
(who may read) is separated from **authentication** (the key). *The governed door.*

**Managed vs. external storage**:
- **Managed** (the Volume fallback) — Databricks owns the storage; drop the table → data gone.
  Simplest; always allowed, even on Free Edition.
- **External** (credential + external location) — data stays in *your* bucket; Databricks just
  references it. You own the files; dropping the table doesn't delete them. The elegant
  cross-cloud read.

**Cross-cloud identity** — Databricks (its host cloud) authenticating to GCP *as a GCP SA* —
identity crossing a cloud boundary.

**Free Edition boundary / feature-gating** — managed platforms unlock features by paid tier.
Creating credentials/external locations is metastore-admin, often **gated** on Free Edition:
the capability exists, your tier can't invoke it. One `create` command tests that gate.

**`spark.read.format("binaryFile")`** — Spark reader that loads each file as **raw bytes**
(one row/file: `path`, `length`, `content`). Needed because `.pb` isn't CSV/JSON — read the
bytes, then decode.

**protobuf / `FeedMessage` / parse step** — Protocol Buffers = compact *binary* serialization.
GTFS-RT wraps data in a `FeedMessage` of many `entity` records. Decode with `gtfs_realtime_pb2`
→ pull `trip_id, vehicle_id, lat, lon, current_status, stop_id, ts`. `current_status` ∈
{INCOMING_AT, STOPPED_AT, IN_TRANSIT_TO} — `STOPPED_AT` a stop at a time = an *actual arrival*,
which is what you compare to the schedule for lateness.

## Glossary quick-reference
- **GTFS** — General Transit Feed Specification; the open standard for transit data (static + realtime).
- **protobuf (`.pb`)** — Protocol Buffers; compact binary format GTFS-RT uses (needs decoding into rows).
- **OTP** — On-Time Performance (our headline metric).
- **medallion (bronze/silver/gold)** — lifecycle stages: raw → cleaned/typed → business-ready marts.
- **partition pruning** — skipping irrelevant data folders at query time to cut cost/latency.
- **idempotency** — safe-to-re-run; same outcome no matter how many times it runs.
- **service account (SA)** — a non-human "robot" identity software uses to authenticate.
- **IAM** — who (member) can do what (role) on which resource; bindings attach them.
- **least privilege** — grant the minimum permission needed (e.g. read-only, not admin).
- **storage credential / external location** — UC's key + governed-path objects for external data.
- **managed vs external** — Databricks owns the storage vs. references data in your own bucket.
- **binaryFile** — Spark reader that loads raw file bytes (for non-tabular formats like `.pb`).
