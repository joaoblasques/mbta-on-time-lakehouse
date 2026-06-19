# Concepts Explained — MBTA On-Time Lakehouse

Plain-language glossary of the DE + transit concepts behind this project, tied to what
we're actually building. Written while learning — see also the project [README](../README.md).

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

## Glossary quick-reference
- **GTFS** — General Transit Feed Specification; the open standard for transit data (static + realtime).
- **protobuf (`.pb`)** — Protocol Buffers; compact binary format GTFS-RT uses (needs decoding into rows).
- **OTP** — On-Time Performance (our headline metric).
- **medallion (bronze/silver/gold)** — lifecycle stages: raw → cleaned/typed → business-ready marts.
- **partition pruning** — skipping irrelevant data folders at query time to cut cost/latency.
- **idempotency** — safe-to-re-run; same outcome no matter how many times it runs.
