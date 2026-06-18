# CLAUDE.md — mbta-on-time-lakehouse

Project context for any AI coding session. Keep it tight; this is read every session.

## What this is
An end-to-end lakehouse measuring **MBTA on-time performance (OTP)** from live
GTFS-Realtime + static schedules. **AWS + Databricks**, reproducible from code.
Stakeholder: transit ops. Metric: OTP % by route/stop/hour. See `README.md`.

## Non-negotiable principles
- **CLI-first / reproducible from code.** Everything (infra, pipelines, jobs) deploys from
  a terminal. Tool versions pinned in `mise.toml`; Python deps via `uv`. If it can't be done
  from code, automate the browser (Playwright); manual GUI is the last resort, documented.
- **Never commit secrets.** This is a PUBLIC repo. Secrets live in `.env` (gitignored) /
  AWS Secrets Manager / GitHub Actions secrets. Check `.gitignore` before adding files.
- **Idempotent pipelines.** Re-runs/backfills must not duplicate data. (Run-id overwrite or
  natural-key upsert.)
- **Data-quality gates before exposing data** — validate source + consumption layers.
- **Cost discipline.** Budget <$20 total. Job/serverless clusters with auto-terminate; no
  24/7 clusters. Tear infra down between sessions. Report run cost.

## Architecture / stack
S3 (bronze, raw) → Spark on Databricks → Delta (**Liquid Clustering**, not Hive partitioning)
→ silver (clean, RT⋈schedule, dedup, lateness) → gold (OTP marts) → Databricks SQL.
Governance: **Unity Catalog**. Orchestration: **Lakeflow Declarative Pipelines**.
Streaming: **Kinesis**. IaC: **Terraform** (AWS + Databricks providers).

## How to work here (lessons baked in)
- **Do NOT one-shot.** A coding agent once silently loaded 1,493 of 5,458 rows and dropped
  columns without comment. Build incrementally; **verify every step against row counts /
  expectations.** Accuracy here is a *context + verification* problem, not a codegen one.
- **Test discipline.** Transforms get unit + integration tests (shared Spark session in
  `tests/conftest.py`). Prefer writing the DQ/transform assertion first.
- **DQ as CI gates**, not vibes — Great Expectations / Soda / native expectations wired into
  GitHub Actions so checks can't be skipped.
- **Separate I/O from transformation logic** so transforms are unit-testable.
- Keep metadata (table format, cluster keys, schema) in version control.

## Layout
`src/ingestion/` pollers · `terraform/` IaC · `databricks/` asset bundles + notebooks-as-code
· `tests/` · `docs/architecture.md` decisions log · `.github/workflows/ci.yml` CI.

## Commands
```bash
mise install && uv sync     # provision toolchain + deps
uv run pytest               # tests
uv run ruff check .         # lint
```

## Out of scope
Databricks/AWS features outside the DE pipeline (ML/MLflow, model serving, web hosting).
An optional text-to-SQL *demo* layer (WrenAI/Vanna) may sit on the finished gold marts — it
is additive, never part of the build.
