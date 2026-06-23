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
- **Streaming / incremental ingestion** — bronze is now **Structured Streaming + Auto Loader**
  (`Trigger.AvailableNow`): processes only *new* files via a checkpoint, keeps full history, stays
  within free-tier limits. Silver windows it. Cut over live with the monitor watching.
- **Cost & FinOps** — a €20 budget alert (50/90/100%) + **per-job DBU measurement** via
  `system.billing.usage`. Near-€0 by design (serverless + incremental).
- **Lakeflow DLT** — spiked a declarative `@dlt.table` + `@dlt.expect` pipeline; it **runs on Free
  Edition** and matched the imperative gold exactly. Kept as a demonstration of both paradigms.

## Next ⏳
- **Certifications** *(optional)* — Databricks DE Associate / GCP Professional Data Engineer.
- Full DLT migration is a possible future direction (production uses Jobs + Asset Bundles today).

## Design principles

- **Demoable early, polish later** — every phase ends with something that runs.
- **Test in dev → promote to prod** — never rewire the live pipeline blind.
- **Document the *why*** — concepts and decisions are written down as they're made.
