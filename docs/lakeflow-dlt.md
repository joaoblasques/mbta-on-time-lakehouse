# Lakeflow Declarative Pipelines (DLT) — spike

**Result: ✅ DLT runs on Databricks Free Edition (serverless).** Verified 2026-06-23.

## What DLT is (and how it differs from our jobs)

With regular **jobs** (what production uses — `databricks/notebooks/0*.py` wired by an Asset
Bundle), *you* write the "how": read a table, transform, write a table, and declare the task order.

With **DLT** you write the "what" — each table is **declared** with a `@dlt.table` decorator, and
Databricks derives the rest:

| | Jobs (our prod) | DLT |
|---|---|---|
| You specify | tables **+ orchestration** (task DAG) | just the **table definitions** |
| Dependencies | declared by hand (`depends_on`) | **inferred** from the queries |
| Data quality | `assert` in the notebook | **`@dlt.expect`** (declarative, tracked, can drop/quarantine) |
| Incremental | you implement (Auto Loader, windows) | **built in** (streaming tables / materialized views) |
| Retries / lineage | job-level | **per-table**, with a lineage graph in the UI |

DLT is the higher-level, managed way to build a medallion. The trade-off: less control, more magic.

## The spike

A minimal declarative slice — `databricks/notebooks/dlt_otp_spike.py`:

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(name="otp_by_route_dlt", comment="OTP per route, declared via DLT")
@dlt.expect("otp_pct_in_range", "otp_pct BETWEEN 0 AND 100")     # warn-only
@dlt.expect_or_drop("has_observations", "observations > 0")      # drop bad rows
def otp_by_route_dlt():
    ...  # read silver → classify on-time → group by route
```

Created a **serverless** pipeline (`catalog: mbta`, `schema: gold`, `development: true`) and ran an
update:

```bash
databricks pipelines create --json '{"name":"mbta-otp-dlt-spike","serverless":true,
  "catalog":"mbta","schema":"gold","development":true,
  "libraries":[{"notebook":{"path":".../dlt_otp_spike"}}]}'
databricks pipelines start-update <pipeline_id>
```

## Findings

- **It works on Free Edition.** The update went `WAITING_FOR_RESOURCES → COMPLETED` in ~50s on
  serverless — no always-on cluster, no extra setup.
- **Output is correct.** `mbta.gold.otp_by_route_dlt` produced **173 routes** — exactly matching
  the imperative `gold.otp_by_route` — with the same OTP shape. The `@dlt.expect_or_drop`
  expectation gated the rows declaratively.

## Productionized (2026-06-25)

The spike is now a production-grade pipeline (built; live deploy deferred while compute is paused):

- **Bundle resource** `resources/otp_dlt.pipeline.yml` (`databricks bundle deploy`), not a
  hand-created `pipelines create`. Serverless, on-demand → €0 idle.
- **Notebook** `databricks/notebooks/dlt_otp_marts.py` — three `@dlt.table` materialized views
  (`otp_by_route_dlt`, `otp_by_route_hour_dlt`, `otp_by_stop_dlt`) that **import the tested wheel**
  (`transforms.otp.by_route` / `by_route_hour` / `by_stop`) — the same functions `05_gold_otp.py`
  calls. No inline logic.
- **Declarative DQ** via `@dlt.expect` (otp in range, counts reconcile, has observations).
- **Equivalent by construction:** because both paradigms call the same builders, the `_dlt` marts
  equal the Jobs `gold.otp_by_*` tables — pinned by `tests/test_otp_marts.py`. A headless
  symmetric-`EXCEPT` check (`verify_dlt_equivalence.py`) confirms it on the live tables at deploy.

**Production still runs on Jobs + Asset Bundles** (decision #17). The DLT pipeline is a parallel,
fully-maintained second paradigm — kept honest by the shared builders and the equivalence check.
