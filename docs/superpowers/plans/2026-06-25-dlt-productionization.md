# DLT Productionization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the throwaway DLT spike into a production-grade, bundle-deployed, wheel-reusing gold-layer Lakeflow pipeline that runs in parallel with the Jobs medallion and is provably equivalent to it.

**Architecture:** A new serverless DLT pipeline (`otp_marts_dlt`) reads the Jobs-produced `mbta.silver.trip_stop_lateness` and declares the three OTP marts as materialized views. Both the Jobs notebook (`05_gold_otp.py`) and the DLT notebook call the *same* shared mart-builder functions in the tested `transforms` wheel, so the two paradigms produce identical tables by construction. The pipeline is declared as an Asset Bundle resource (not hand-created) and runs on-demand (€0 idle). Decision #17 (Jobs stays prod) is preserved.

**Tech Stack:** Databricks Lakeflow Declarative Pipelines (`dlt`), PySpark, Databricks Asset Bundles, the `transforms` wheel (hatchling), pytest + local SparkSession.

## Global Constraints

- **Reuse the tested wheel — zero logic duplication.** The DLT notebook and `05_gold_otp.py` MUST both call the same functions from `transforms.otp`. No OTP logic inline in any notebook.
- **Bundle-deployed only.** The pipeline is a `resources/*.yml` resource deployed via `databricks bundle deploy`. Never `databricks pipelines create`.
- **Serverless, on-demand, €0 idle.** `serverless: true`; no schedule on any target.
- **Public repo — never commit secrets.** Auth is via the `mbta` Databricks CLI profile (already configured); nothing secret enters the repo.
- **Wheel path convention:** `../dist/*.whl` relative to the `resources/` file (matches `resources/medallion.job.yml:21`).
- **Mart table names:** `mbta.gold.otp_by_route_dlt`, `mbta.gold.otp_by_route_hour_dlt`, `mbta.gold.otp_by_stop_dlt`.
- **OTP band:** `EARLY_BOUND=-1.0`, `LATE_BOUND=5.0` minutes (already in `transforms.otp`; do not redefine).
- **Stop mart minimum observations:** `>= 20` (mirrors `05_gold_otp.py`).
- **Run notebooks/pipelines headless** via `databricks ... -p mbta`; verify against row counts.
- **Vault write-ups are out of scope here** — handed off to the brain session (single-writer rule).

---

### Task 1: Extract shared mart-builder functions into `transforms.otp`

Adds the single source of truth for the three mart definitions (dims, the stop filter, ordering). Both paradigms will call these. TDD: pin them first.

**Files:**
- Modify: `src/transforms/otp.py` (append three functions)
- Test: `tests/test_otp_marts.py` (create)

**Interfaces:**
- Consumes: existing `classify(df) -> DataFrame` and `otp_agg(df, dims) -> DataFrame` in `transforms.otp`.
- Produces (later tasks rely on these exact names/signatures):
  - `by_route(lateness: DataFrame) -> DataFrame` — classifies `lateness` then `otp_agg` over `["route_id","route_short_name","route_long_name"]`, ordered by `otp_pct`.
  - `by_route_hour(lateness: DataFrame) -> DataFrame` — over `["route_id","route_short_name","hour"]`, ordered by `route_id, hour`.
  - `by_stop(lateness: DataFrame, min_obs: int = 20) -> DataFrame` — over `["stop_id","stop_name"]`, filtered to `observations >= min_obs`, ordered by `otp_pct`.
  - Each takes **raw** (un-classified) lateness and classifies internally — so a DLT MV can read silver and call one function.

- [ ] **Step 1: Write the failing test**

Create `tests/test_otp_marts.py`:

```python
"""Shared OTP mart builders (transforms.otp.by_route / by_route_hour / by_stop).

Both the Jobs notebook (05_gold_otp.py) and the DLT notebook (dlt_otp_marts.py) call these
exact functions, so pinning them here guarantees the two paradigms produce identical marts.
"""

from src.transforms.otp import by_route, by_route_hour, by_stop

# silver.trip_stop_lateness-shaped columns the builders need. actual_secs_adj 29100 = 08:05 → hour 8.
SILVER = ("route_id string, route_short_name string, route_long_name string, "
          "stop_id string, stop_name string, lateness_min double, actual_secs_adj long")


def _rows():
    # R1/S1: 4 observations → 2 on-time (2.0, 0.0), 1 late (10.0), 1 early (-3.0) → OTP 50%.
    return [
        ("R1", "1", "Route One", "S1", "Stop S1", 2.0, 29100),
        ("R1", "1", "Route One", "S1", "Stop S1", 10.0, 29100),
        ("R1", "1", "Route One", "S1", "Stop S1", -3.0, 29100),
        ("R1", "1", "Route One", "S1", "Stop S1", 0.0, 29100),
    ]


def test_by_route_rolls_up_otp(spark):
    df = spark.createDataFrame(_rows(), SILVER)
    r = {x["route_id"]: x for x in by_route(df).collect()}["R1"]
    assert (r["observations"], r["on_time_n"], r["late_n"], r["early_n"]) == (4, 2, 1, 1)
    assert r["otp_pct"] == 50.0


def test_by_route_hour_derives_hour(spark):
    df = spark.createDataFrame(_rows(), SILVER)
    r = by_route_hour(df).collect()[0]
    assert (r["route_id"], r["hour"], r["observations"]) == ("R1", 8, 4)


def test_by_stop_applies_min_obs_filter(spark):
    df = spark.createDataFrame(_rows(), SILVER)
    assert by_stop(df, min_obs=20).count() == 0          # 4 obs < 20 → dropped
    kept = {x["stop_id"]: x for x in by_stop(df, min_obs=1).collect()}["S1"]
    assert kept["observations"] == 4 and kept["otp_pct"] == 50.0
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_otp_marts.py -q`
Expected: FAIL with `ImportError: cannot import name 'by_route' from 'src.transforms.otp'`.

- [ ] **Step 3: Add the three builders to `src/transforms/otp.py`**

Append to `src/transforms/otp.py` (after `otp_agg`):

```python
def by_route(lateness: DataFrame) -> DataFrame:
    """OTP per route. Takes raw lateness (un-classified); classifies internally."""
    return otp_agg(classify(lateness),
                   ["route_id", "route_short_name", "route_long_name"]).orderBy("otp_pct")


def by_route_hour(lateness: DataFrame) -> DataFrame:
    """OTP per route × hour-of-day. Takes raw lateness; classifies internally."""
    return otp_agg(classify(lateness),
                   ["route_id", "route_short_name", "hour"]).orderBy("route_id", "hour")


def by_stop(lateness: DataFrame, min_obs: int = 20) -> DataFrame:
    """OTP per stop, requiring `min_obs` observations to be meaningful. Takes raw lateness."""
    return (otp_agg(classify(lateness), ["stop_id", "stop_name"])
            .filter(F.col("observations") >= min_obs)
            .orderBy("otp_pct"))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_otp_marts.py -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Lint + full suite**

Run: `uv run ruff check . && uv run pytest -q`
Expected: ruff clean; all tests pass (existing + 3 new).

- [ ] **Step 6: Commit**

```bash
git add src/transforms/otp.py tests/test_otp_marts.py
git commit -m "feat(transforms): shared OTP mart builders (by_route/by_route_hour/by_stop)

Single source of truth for the three gold marts (dims, stop min-obs filter, ordering),
so the Jobs notebook and the upcoming DLT notebook produce identical tables by construction.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01XKMZhwKtRnyuHdQ3ai3yCm"
```

---

### Task 2: Refactor `05_gold_otp.py` to call the shared builders

Removes the inline mart logic from the Jobs notebook so it uses the same functions DLT will. Behavior is identical (the wheel rebuilds on deploy).

**Files:**
- Modify: `databricks/notebooks/05_gold_otp.py:16` (import) and `:20-31` (mart writes)

**Interfaces:**
- Consumes: `by_route`, `by_route_hour`, `by_stop`, `classify` from `transforms.otp` (Task 1).

- [ ] **Step 1: Replace the import line**

In `databricks/notebooks/05_gold_otp.py`, replace:

```python
from transforms.otp import classify, otp_agg  # tested wheel (deployed by the Asset Bundle)
```

with:

```python
from transforms.otp import by_route, by_route_hour, by_stop, classify  # tested wheel (Asset Bundle)
```

- [ ] **Step 2: Replace the mart-writing cell**

Replace the cell that currently reads (lines ~20-31):

```python
# COMMAND ----------
L = classify(spark.table("mbta.silver.trip_stop_lateness"))  # adds on_time/is_late/is_early/hour

otp_agg(L, ["route_id", "route_short_name", "route_long_name"]).orderBy("otp_pct") \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_route")

otp_agg(L, ["route_id", "route_short_name", "hour"]).orderBy("route_id", "hour") \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_route_hour")

# worst stops = where lateness concentrates (require min observations to be meaningful)
otp_agg(L, ["stop_id", "stop_name"]).filter(F.col("observations") >= 20).orderBy("otp_pct") \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_stop")
```

with:

```python
# COMMAND ----------
RAW = spark.table("mbta.silver.trip_stop_lateness")           # raw lateness; builders classify internally

by_route(RAW) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_route")

by_route_hour(RAW) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_route_hour")

by_stop(RAW) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("mbta.gold.otp_by_stop")

L = classify(RAW)                                             # classified frame for the run summary below
```

(The trailing-summary cell still references `L` and `F`; `L` is now defined here and `F`/`classify` imports remain.)

- [ ] **Step 3: Lint**

Run: `uv run ruff check databricks/notebooks/05_gold_otp.py`
Expected: clean (no unused-import; `otp_agg` is gone, `by_*` + `classify` used; `F` still used in DQ cell).

- [ ] **Step 4: Commit**

```bash
git add databricks/notebooks/05_gold_otp.py
git commit -m "refactor(gold): 05_gold_otp uses shared transforms.otp mart builders

No behavior change — the three marts now come from by_route/by_route_hour/by_stop, the same
functions the DLT pipeline will call (single source of truth).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01XKMZhwKtRnyuHdQ3ai3yCm"
```

---

### Task 3: Create the DLT notebook `dlt_otp_marts.py` and retire the spike

The production DLT source: three `@dlt.table` materialized views, each importing the wheel and calling a shared builder, with declarative `@dlt.expect` DQ.

**Files:**
- Create: `databricks/notebooks/dlt_otp_marts.py`
- Delete: `databricks/notebooks/dlt_otp_spike.py`

**Interfaces:**
- Consumes: `by_route`, `by_route_hour`, `by_stop` from `transforms.otp` (Task 1); the runtime-injected `spark` global.

- [ ] **Step 1: Create `databricks/notebooks/dlt_otp_marts.py`**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # DLT — OTP marts (Lakeflow Declarative Pipelines, productionized)
# MAGIC Declares the three gold OTP marts as materialized views over the Jobs-produced
# MAGIC `mbta.silver.trip_stop_lateness`. The OTP logic is **imported from the tested wheel**
# MAGIC (`transforms.otp` — the same functions `05_gold_otp.py` calls), so the DLT marts equal the
# MAGIC Jobs marts by construction. Data quality is declarative via `@dlt.expect`. Runs serverless,
# MAGIC on-demand (€0 idle). See `docs/lakeflow-dlt.md` + arch decision #18.

# COMMAND ----------
import dlt

from transforms.otp import by_route, by_route_hour, by_stop  # tested wheel (attached by the bundle)

SILVER = "mbta.silver.trip_stop_lateness"


@dlt.table(name="otp_by_route_dlt", comment="OTP per route (declarative). Mirrors gold.otp_by_route.")
@dlt.expect("otp_pct_in_range", "otp_pct BETWEEN 0 AND 100")
@dlt.expect("counts_reconcile", "on_time_n + late_n + early_n = observations")
@dlt.expect_or_drop("has_observations", "observations > 0")
def otp_by_route_dlt():
    return by_route(spark.read.table(SILVER))


@dlt.table(name="otp_by_route_hour_dlt",
           comment="OTP per route × hour (declarative). Mirrors gold.otp_by_route_hour.")
@dlt.expect("otp_pct_in_range", "otp_pct BETWEEN 0 AND 100")
@dlt.expect("counts_reconcile", "on_time_n + late_n + early_n = observations")
@dlt.expect_or_drop("has_observations", "observations > 0")
def otp_by_route_hour_dlt():
    return by_route_hour(spark.read.table(SILVER))


@dlt.table(name="otp_by_stop_dlt", comment="OTP per stop, ≥20 obs (declarative). Mirrors gold.otp_by_stop.")
@dlt.expect("otp_pct_in_range", "otp_pct BETWEEN 0 AND 100")
@dlt.expect("counts_reconcile", "on_time_n + late_n + early_n = observations")
@dlt.expect_or_drop("has_observations", "observations > 0")
def otp_by_stop_dlt():
    return by_stop(spark.read.table(SILVER))
```

- [ ] **Step 2: Delete the spike notebook**

```bash
git rm databricks/notebooks/dlt_otp_spike.py
```

- [ ] **Step 3: Lint**

Run: `uv run ruff check databricks/notebooks/dlt_otp_marts.py`
Expected: clean (`dlt`/`spark` F821 ignored by the `databricks/notebooks/*.py` per-file rule).

- [ ] **Step 4: Commit**

```bash
git add databricks/notebooks/dlt_otp_marts.py
git commit -m "feat(dlt): productionized OTP marts notebook (imports tested wheel, @dlt.expect DQ)

Three @dlt.table materialized views over silver, each calling the shared transforms.otp
builders — no inline logic. Declarative DQ via @dlt.expect. Retires dlt_otp_spike.py.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01XKMZhwKtRnyuHdQ3ai3yCm"
```

---

### Task 4: Declare the DLT pipeline as a bundle resource

Makes the pipeline reproducible from code (replaces `pipelines create --json`).

**Files:**
- Create: `resources/otp_dlt.pipeline.yml`

**Interfaces:**
- Consumes: the `transforms` wheel built by `databricks.yml`'s `artifacts.transforms_wheel`; the notebook from Task 3.

- [ ] **Step 1: Create `resources/otp_dlt.pipeline.yml`**

```yaml
# The productionized DLT (Lakeflow Declarative Pipeline) for the gold OTP marts, as a bundle
# resource (deployed via `databricks bundle deploy`, never `pipelines create`). Serverless,
# on-demand (no schedule → €0 idle). The bundle attaches the tested `transforms` wheel via the
# pipeline environment, so the DLT notebook imports the same code as the Jobs path.
#
#   databricks bundle deploy -t dev  -p mbta
#   databricks bundle run otp_marts_dlt -t dev -p mbta

resources:
  pipelines:
    otp_marts_dlt:
      name: mbta-otp-marts-dlt
      catalog: mbta
      schema: gold
      serverless: true
      # Attach the bundle-built tested wheel (same artifact the medallion job uses).
      environment:
        dependencies:
          - ../dist/*.whl # relative to this file (resources/) → repo-root/dist/
      libraries:
        - notebook:
            path: ../databricks/notebooks/dlt_otp_marts.py
```

(`bundle.mode: development` on the `dev` target auto-sets the pipeline's `development: true` and name-prefixes it; `prod` deploys clean names. No schedule on either → on-demand.)

- [ ] **Step 2: Validate the bundle (both targets)**

Run:
```bash
databricks bundle validate -t dev  -p mbta
databricks bundle validate -t prod -p mbta
```
Expected: both print the resolved config with a `pipelines.otp_marts_dlt` resource and no errors. If `../dist/*.whl` errors as "not found", run `uv build --wheel` once (the bundle normally builds it on deploy) and re-validate.

- [ ] **Step 3: Commit**

```bash
git add resources/otp_dlt.pipeline.yml
git commit -m "feat(dlt): declare otp_marts_dlt pipeline as an Asset Bundle resource

Serverless, on-demand, wheel attached via the pipeline environment. Reproducible from code —
replaces the hand-created spike pipeline.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01XKMZhwKtRnyuHdQ3ai3yCm"
```

---

### Task 5: Deploy, run, and verify live equivalence (headless)

Operationalizes success criterion #4: the `_dlt` marts must equal the Jobs marts exactly. This is a live Databricks step (not CI) — run against the `mbta` profile. A verification notebook makes the equality claim reproducible from code. The DLT pipeline writes to the declared `mbta.gold` schema regardless of target (dev mode prefixes the pipeline *name*, not the output tables), so the `_dlt` tables land at `mbta.gold.otp_by_*_dlt` and never collide with the prod Jobs marts (`mbta.gold.otp_by_*`, no `_dlt`).

**Files:**
- Create: `databricks/notebooks/verify_dlt_equivalence.py`

**Interfaces:**
- Consumes: the deployed pipeline (Task 4) and the Jobs gold marts (already live).

- [ ] **Step 1: Create the verification notebook `databricks/notebooks/verify_dlt_equivalence.py`**

(Created before deploy so `bundle deploy` syncs it to the workspace.)

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Verify DLT ≡ Jobs marts
# MAGIC Asserts each `otp_by_*_dlt` materialized view equals its Jobs `gold.otp_by_*` table exactly
# MAGIC (same row count + symmetric `EXCEPT` empty). Run after a DLT pipeline update. Headless via
# MAGIC `databricks jobs submit`. Both live in `mbta.gold`.

# COMMAND ----------
import json

PAIRS = [("otp_by_route", "otp_by_route_dlt"),
         ("otp_by_route_hour", "otp_by_route_hour_dlt"),
         ("otp_by_stop", "otp_by_stop_dlt")]

results = {}
for jobs_name, dlt_name in PAIRS:
    jobs = spark.table(f"mbta.gold.{jobs_name}")
    dlt_tbl = spark.table(f"mbta.gold.{dlt_name}")
    n_jobs, n_dlt = jobs.count(), dlt_tbl.count()
    only_jobs = jobs.exceptAll(dlt_tbl).count()
    only_dlt = dlt_tbl.exceptAll(jobs).count()
    ok = (n_jobs == n_dlt) and only_jobs == 0 and only_dlt == 0
    results[jobs_name] = {"jobs": n_jobs, "dlt": n_dlt, "only_jobs": only_jobs,
                          "only_dlt": only_dlt, "equal": ok}
    print(("OK  " if ok else "FAIL"), jobs_name, results[jobs_name])
    assert ok, f"DLT != Jobs for {jobs_name}: {results[jobs_name]}"

dbutils.notebook.exit(json.dumps(results))
```

- [ ] **Step 2: Deploy the bundle (dev) — builds the wheel + creates the pipeline + syncs notebooks**

Run: `databricks bundle deploy -t dev -p mbta`
Expected: builds `dist/*.whl`, uploads the notebooks (incl. `dlt_otp_marts` and `verify_dlt_equivalence`), creates/updates pipeline `[dev <user>] mbta-otp-marts-dlt`.

- [ ] **Step 3: Run the DLT pipeline**

Run: `databricks bundle run otp_marts_dlt -t dev -p mbta`
Expected: update goes `WAITING_FOR_RESOURCES → COMPLETED`; creates `mbta.gold.otp_by_route_dlt`, `mbta.gold.otp_by_route_hour_dlt`, `mbta.gold.otp_by_stop_dlt`.

- [ ] **Step 4: Run the verification headless**

```bash
databricks jobs submit -p mbta --json '{
  "run_name": "verify-dlt-equivalence",
  "tasks": [{
    "task_key": "verify",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/tilakapash@gmail.com/.bundle/mbta-on-time-lakehouse/dev/files/databricks/notebooks/verify_dlt_equivalence"
    },
    "environment_key": "default"
  }],
  "environments": [{"environment_key": "default", "spec": {"client": "3"}}]
}'
```
Expected: run SUCCESS; output JSON shows `equal: true` for all three pairs (`only_jobs=0`, `only_dlt=0`, matching counts). If the deployed file path differs, get it from `databricks bundle deploy` output or the workspace; the `.bundle/<name>/dev/files/...` prefix is the standard sync location.

- [ ] **Step 5: Commit**

```bash
git add databricks/notebooks/verify_dlt_equivalence.py
git commit -m "test(dlt): headless verification that DLT marts == Jobs marts

Symmetric EXCEPT + row-count check across the three mart pairs. Run after a pipeline update.
Verified live: all three equal.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01XKMZhwKtRnyuHdQ3ai3yCm"
```

---

### Task 6: Update the docs (arch decision #18 + `lakeflow-dlt.md`)

**Files:**
- Modify: `docs/architecture.md` (append decision #18 after #17)
- Modify: `docs/lakeflow-dlt.md` (spike → productionized)

- [ ] **Step 1: Append decision #18 to `docs/architecture.md`**

After the `### 17. ...` block and before `## Known limitations`, insert:

```markdown
### 18. Lakeflow DLT, productionized — a parallel, proven-equivalent gold pipeline
**Decision:** the DLT spike (#17) is productionized into a bundle-deployed serverless Lakeflow
pipeline (`otp_marts_dlt`, `resources/otp_dlt.pipeline.yml`) that declares the three gold OTP marts
as materialized views with `@dlt.expect` DQ. It runs **in parallel** with the Jobs medallion
(decision #17 stands — Jobs is still prod), reading the same `silver.trip_stop_lateness`. **Both
paradigms import the same `transforms.otp` mart builders** (`by_route`/`by_route_hour`/`by_stop`),
so the `_dlt` marts equal the Jobs marts **by construction** — verified by a headless symmetric-
`EXCEPT` check (`verify_dlt_equivalence.py`). On-demand, serverless → €0 idle. **Why parallel not
replace:** the Jobs path (Auto Loader streaming, the OOM fix, tested wheel, monitor integration) is
the project's strongest engineering; the value of DLT here is demonstrating both paradigms and
proving them equivalent, not discarding working prod. **Tradeoff:** two gold-producing paths exist,
but the shared builders + equivalence check mean they cannot silently drift. See
`docs/lakeflow-dlt.md`.
```

- [ ] **Step 2: Rewrite the "Decision" section of `docs/lakeflow-dlt.md`**

Replace the final `## Decision` section of `docs/lakeflow-dlt.md` with:

```markdown
## Productionized (2026-06-25)

The spike is now a production-grade pipeline:

- **Bundle resource** `resources/otp_dlt.pipeline.yml` (`databricks bundle deploy`), not a
  hand-created `pipelines create`. Serverless, on-demand → €0 idle.
- **Notebook** `databricks/notebooks/dlt_otp_marts.py` — three `@dlt.table` materialized views
  (`otp_by_route_dlt`, `otp_by_route_hour_dlt`, `otp_by_stop_dlt`) that **import the tested wheel**
  (`transforms.otp.by_route` / `by_route_hour` / `by_stop`) — the same functions `05_gold_otp.py`
  calls. No inline logic.
- **Declarative DQ** via `@dlt.expect` (otp in range, counts reconcile, has observations).
- **Proven equivalent:** `verify_dlt_equivalence.py` asserts each `_dlt` mart equals its Jobs
  `gold.otp_by_*` table (symmetric `EXCEPT` empty + matching counts).

**Production still runs on Jobs + Asset Bundles** (decision #17). The DLT pipeline is a parallel,
fully-maintained second paradigm — kept honest by the shared builders and the equivalence check.
```

- [ ] **Step 3: Verify docs build (if MkDocs strict is wired) / sanity check links**

Run: `grep -n "### 18" docs/architecture.md && grep -n "Productionized" docs/lakeflow-dlt.md`
Expected: both match — the new sections exist.

- [ ] **Step 4: Commit**

```bash
git add docs/architecture.md docs/lakeflow-dlt.md
git commit -m "docs(dlt): decision #18 + lakeflow-dlt.md — productionized parallel pipeline

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01XKMZhwKtRnyuHdQ3ai3yCm"
```

---

## Post-implementation (out of plan, noted for handoff)

- **Vault updates** (Roadmap "what's left" #2/#8 → DLT productionized; decision journal) are handed off to the **shared brain session** — do NOT commit the vault from this repo session.
- Optional follow-up: wire `verify_dlt_equivalence.py` into a scheduled check or the failure-monitor; not in scope here.
