# Roadmap

What's built, and what's next. (Source of truth: the repo's `Roadmap.md`.)

## Done ✅

- **End-to-end pipeline** — MBTA live feed → GCS → Databricks → Bronze → Silver → Gold OTP.
- **Dashboard** — AI/BI OTP views by route, hour, and stop.
- **Rigor** — data-quality gates + unit & **Spark integration tests** (incl. the after-midnight wrap).
- **Self-managing layer** — the nightly **Dreamer** (insights → PRs) and the **Monitor** (auto-retry / auto-issue), with tiered autonomy.
- **Reproducible infra** — **Terraform** (GCP) + **Databricks Asset Bundles** (the job + a tested transforms **wheel** the notebooks import).
- **CI/CD** — lint + tests on every PR; `terraform plan → apply` gated by **keyless Workload Identity Federation** (no stored keys).
- **Scaling fix** — incremental copier + bounded, distributed RT decoding.

## Next ⏳

- **Streaming / incremental ingestion** — Structured Streaming + **Auto Loader** with
  `Trigger.AvailableNow`: process only *new* files via a checkpoint, keep full history, stay within
  free-tier limits (no always-on cluster). Built dev-first, then cut over.
- **Lakeflow Declarative Pipelines (DLT)** — express the medallion declaratively *(stretch — runtime
  availability on Free Edition to be confirmed)*.
- **Cost recording** — capture per-run cost + a budget alert.
- **Certifications** *(optional)* — Databricks DE Associate / GCP Professional Data Engineer.

## Design principles

- **Demoable early, polish later** — every phase ends with something that runs.
- **Test in dev → promote to prod** — never rewire the live pipeline blind.
- **Document the *why*** — concepts and decisions are written down as they're made.
