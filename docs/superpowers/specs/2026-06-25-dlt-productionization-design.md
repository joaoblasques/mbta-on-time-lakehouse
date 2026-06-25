# Design: Productionize the DLT spike → `otp_marts_dlt` Lakeflow pipeline

**Date:** 2026-06-25
**Status:** Approved (brainstorming) — pending implementation plan
**Related:** arch decisions #11 (Asset Bundles), #14 (tested transforms wheel), #17 (DLT spike);
`docs/lakeflow-dlt.md`, `databricks/notebooks/dlt_otp_spike.py`

## Problem

The Lakeflow DLT spike (#17) proved a serverless Declarative Pipeline runs on Free Edition, but it
is not production-grade. It has three concrete flaws:

1. **It forks the logic.** `dlt_otp_spike.py` reimplements the OTP classification/aggregation
   inline instead of importing the tested wheel (`transforms.otp`). Drift risk: the spike and prod
   can silently diverge.
2. **It was hand-created** via `databricks pipelines create --json` — not version-controlled, not
   reproducible from the bundle, not dev/prod-targeted.
3. **Equivalence to prod is a one-off manual check** (the spike's "173 routes matched"), not a
   repeatable test.

## Goal

A genuinely production-grade DLT pipeline that fixes all three flaws, **without reversing decision
#17** (Jobs + Asset Bundles remain the live prod path). The DLT pipeline runs in parallel as a
fully-maintained second paradigm — the dual-paradigm story is itself the portfolio asset.

## Decisions (settled in brainstorming)

- **Coexistence: parallel, maintained.** Jobs stays prod. DLT is a real, bundle-deployed second
  pipeline. Rationale: the Jobs path (Auto Loader streaming bronze, the protobuf-UDF OOM fix,
  bounded windows, the tested wheel, 27 tests, failure-monitor integration) is the project's
  strongest engineering — replacing it with DLT's managed abstraction would *remove* demonstrated
  depth. "I run a hand-tuned streaming Jobs pipeline in prod AND a declarative Lakeflow pipeline,
  and chose Jobs deliberately" beats "I use DLT."
- **Scope: gold-only.** DLT reads the Jobs-produced `mbta.silver.trip_stop_lateness` and declares
  the three OTP marts as materialized views. Silver is **excluded** because it is non-deterministic
  (`feed_ts >= now()-3d` window — disallowed in DLT MVs) and stateful (dedup-to-latest window
  function — not append-only), and forcing it into DLT would either drop the deliberate
  recent-window OTP semantics (#12/#15) or recompute 32M rows per run. Gold marts are pure
  deterministic aggregations — textbook DLT materialized views. **Decisive tiebreaker:** reading the
  *same* Jobs silver makes "DLT marts == Jobs marts" a clean exact-equality invariant; a separate
  DLT silver would make the equivalence fuzzy.
- **Idle cost: €0.** Triggered serverless, run on-demand — never continuous.

## Architecture

```
                 (Jobs medallion — UNCHANGED, live prod)
  bronze.rt_trip_updates → silver.trip_stop_lateness → gold.otp_by_route / _route_hour / _by_stop
                                     │
                                     │  (DLT reads the same silver)
                                     ▼
                 (DLT pipeline — new, parallel)
                          classify() → otp_agg()        [from the tested wheel]
                                     │
                                     ▼
              gold.otp_by_route_dlt / otp_by_route_hour_dlt / otp_by_stop_dlt
                          (materialized views, @dlt.expect DQ)
```

## Components

### 1. `databricks/notebooks/dlt_otp_marts.py` (new — replaces `dlt_otp_spike.py`)

Three `@dlt.table` materialized views, each **importing the tested wheel** (no inline logic):

- `otp_by_route_dlt` — `otp_agg(classify(silver), ["route_id","route_short_name","route_long_name"])`
- `otp_by_route_hour_dlt` — `otp_agg(classify(silver), ["route_id","route_short_name","hour"])`
- `otp_by_stop_dlt` — `otp_agg(classify(silver), ["stop_id","stop_name"])` filtered to
  `observations >= 20` (mirrors `05_gold_otp.py`)

`classify` and `otp_agg` come from `transforms.otp` — the same functions the Jobs notebook
`05_gold_otp.py` calls. Single source of truth.

`dlt_otp_spike.py` is **deleted** to avoid two competing DLT notebooks.

### 2. `resources/otp_dlt.pipeline.yml` (new bundle resource)

Declares the DLT pipeline as code (replaces the `pipelines create --json` invocation):

- `serverless: true`, `catalog: mbta`, `schema: gold`
- libraries → `dlt_otp_marts.py`
- the tested wheel attached via the pipeline's serverless environment/library spec (same wheel
  `databricks.yml` builds for the job)
- dev/prod targets consistent with the existing bundle: `dev` uses `development: true`
  (name-prefixed, on-demand); `prod` clean names, **on-demand** (no schedule). Rationale: the
  Jobs medallion already refreshes gold hourly; scheduling a duplicate DLT refresh adds cost/noise
  for a demonstration pipeline with no value. Run via `databricks bundle run` when demonstrating.
- deployed via `databricks bundle deploy` — never hand-created

### 3. Data quality — declarative `@dlt.expect`

Mirror the imperative `assert` gates from `05_gold_otp.py` as tracked expectations:

- `@dlt.expect("otp_pct_in_range", "otp_pct BETWEEN 0 AND 100")` (warn)
- `@dlt.expect_or_drop("has_observations", "observations > 0")` (drop)
- counts reconcile: `on_time_n + late_n + early_n = observations` (expect)

Visible in the DLT event log + lineage UI.

## Data flow

`silver.trip_stop_lateness` (Jobs-owned) → DLT reads → `classify()` adds
on_time/is_late/is_early/hour → `otp_agg(dims)` per mart → `@dlt.expect` DQ → three `_dlt`
materialized views in `mbta.gold`. One-hop lineage: one source fanning out to three marts.

## Testing

- **In CI (fast, no Databricks): transform-level equivalence.** Because both the DLT notebook and
  the Jobs notebook call the same `transforms.otp` functions on the same logical input, a unit test
  on a local SparkSession asserting the mart outputs are identical for a fixed silver fixture covers
  the divergence risk deterministically. This extends the existing `tests/` Spark suite (#14).
- **Post-deploy (manual/documented verify): live table equality.** After a DLT pipeline run, assert
  `mbta.gold.otp_by_*_dlt` equals `mbta.gold.otp_by_*` exactly (row count + value equality). DLT
  tables only exist after a pipeline update, so this is a documented verification step, not a CI
  gate. Run headless via the bundle.

## Documentation

- Update `docs/lakeflow-dlt.md`: spike → productionized pipeline.
- Add arch decision #18 to `docs/architecture.md` (parallel DLT pipeline, gold-only, wheel reuse,
  proven-equivalent invariant).
- Vault Roadmap / decision-journal updates are **handed off to the shared brain session** — not
  committed from this repo session (single-writer rule).

## Out of scope

- Silver or bronze in DLT.
- Replacing or modifying any Jobs medallion task.
- Continuous-streaming DLT mode.
- Migrating the dashboard / serving layer to the `_dlt` tables.

## Success criteria

1. `databricks bundle deploy` creates/updates the DLT pipeline (no manual `pipelines create`).
2. A pipeline run produces the three `_dlt` marts on serverless, €0 idle, with `@dlt.expect` DQ
   visible in the event log.
3. The DLT notebook imports the tested wheel — zero inline logic duplication.
4. CI transform-equivalence test passes; post-deploy live table equality verified.
5. `dlt_otp_spike.py` removed; docs updated (#18 + `lakeflow-dlt.md`).
