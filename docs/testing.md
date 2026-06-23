# Testing

```bash
uv run pytest -q          # all tests (unit + Spark integration). Needs Java 17+ for the Spark ones.
```

Two layers:

## Unit tests (no Spark, fast)
Pure-Python logic with no cluster: the **OTP Dreamer** (`detect`/`baseline`/`llm` fallback/
`github_pr`), the **failure-monitor** decision logic (`decide`), and the **poller**. These run in
milliseconds and need no JVM.

## Integration tests (real local Spark)
`tests/test_transforms_integration.py` exercises the **silver/gold transforms** on a genuine local
`SparkSession` (shared, session-scoped — one JVM for the whole run; see `tests/conftest.py`,
pinned to UTC for deterministic epoch→timestamp casts). They pin the parts most likely to be
subtly wrong:

- **Dedup to latest prediction** — many RT snapshots per (trip, stop); the max-`feed_ts` row wins.
- **Epoch-UTC → local-service-day reconciliation** — RT stores an absolute UTC instant; the
  schedule is seconds-after-local-midnight. A 5-min-late arrival must read as `+5.0`, an early one
  as negative.
- **After-midnight (>24:00:00) wrap** — a trip scheduled `24:30:00` whose actual arrives `00:35`
  next local day must read as `+5.0` late, not hugely early. This is the trickiest case.
- **OTP on-time band** — `[early, late]` classification + the `otp_agg` rollup (counts reconcile,
  `otp_pct` correct, hour-of-day derived).

## Where the logic lives (single source of truth — with a caveat)
The transforms are extracted into **`src/transforms/`** (`lateness.py`, `otp.py`) as pure
`DataFrame → DataFrame` functions, so the tests exercise the **real** logic, not a paraphrase.

**No drift (resolved 2026-06-23):** the notebooks `04_silver_lateness` and `05_gold_otp` **import
the tested module** — the Asset Bundle builds `src/transforms` into a dep-free wheel
(`artifacts` in `databricks.yml`) and attaches it to the job's **serverless environment**
(`environment_key`), so the silver/gold tasks run `from transforms.lateness import …` /
`from transforms.otp import …`. The code that's tested is the code that runs. Validated in the
bundle's `dev` target, then promoted to `prod`. See `docs/asset-bundles.md`.

## CI
`ci.yml` installs deps (`uv sync --all-groups`, which includes `pyspark`) + **Java 17**
(`actions/setup-java`) and runs `pytest` — so the Spark integration tests run on every PR/push.
