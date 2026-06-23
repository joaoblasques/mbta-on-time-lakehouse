# Cost & FinOps

How this project stays near-€0, how cost is **measured**, and the **guardrail** that stops surprises.

## What costs money (the two surfaces)

| Surface | What's billed | How |
|---|---|---|
| **Databricks** | compute, in **DBUs** (Databricks Units) + a little storage (DSU) | serverless, per-second |
| **GCP** | Cloud Run jobs, GCS storage, Scheduler, Secret Manager, egress | per-use |

A **DBU** is Databricks' unit of compute consumption — you're billed `DBUs × $/DBU` for the tier
(Jobs Serverless, SQL Serverless, …). On **Free Edition the DBU rate is €0**, so usage is *metered
but not charged* — which is exactly why this whole system runs for nothing.

## The guardrail: a budget alert

A **€20 project-scoped budget** on the billing account fires email alerts at **50% / 90% / 100%**
of spend — a tripwire so a runaway job can't quietly burn credits. Created with:

```bash
gcloud billing budgets create --billing-account=<ACCOUNT> \
  --display-name="mbta-on-time-lakehouse guardrail" --budget-amount=20EUR \
  --filter-projects=projects/mbta-on-time-lakehouse \
  --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0
```

## Measuring cost: Databricks `system.billing.usage`

Databricks exposes usage as a queryable system table (works on Free Edition). **Per-job
attribution** via `usage_metadata.job_id` is the key — it answers *"what does one job cost?"*

```sql
-- DBU by job, last 7 days (the medallion job is the one to watch)
SELECT usage_metadata.job_id AS job_id,
       round(sum(usage_quantity), 1) AS dbu
FROM   system.billing.usage
WHERE  sku_name LIKE '%JOBS_SERVERLESS%'
  AND  usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY 1 ORDER BY dbu DESC;

-- daily usage by SKU (compute vs SQL vs storage)
SELECT usage_date, sku_name, round(sum(usage_quantity), 2) AS qty, max(usage_unit) AS unit
FROM   system.billing.usage
WHERE  usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY 1, 2 ORDER BY 1 DESC, qty DESC;
```

## Recorded snapshot (2026-06-23)

- **Total ≈ 86 DBU / 7 days** (serverless).
- **Medallion job (bundle, `390…`): ≈ 21 DBU / 3 days.**
- Honest note: recent DBUs are **inflated by one-time spikes** — the streaming **cutover backfill**
  (1,657 files in one go) and earlier concurrency-error reruns. **Steady-state** hourly medallion
  runs on small incremental windows are far cheaper.
- At pay-as-you-go rates (Jobs Serverless is on the order of ~€0.30–0.55/DBU depending on tier/region),
  this would be a few euros per week — **on Free Edition it's €0.** The deliverable here is the
  *measurement + attribution*, which is what you'd do on a paid workspace to control spend.

## GCP side

Tiny: Cloud Run jobs are short serverless invocations; GCS holds small `.pb` files; Scheduler +
Secret Manager are negligible. All comfortably inside the €20 budget and the free credits. Pause
everything anytime with `gcloud scheduler jobs pause …` (see `docs/getting-started`).

## FinOps practices demonstrated

- **Budget + threshold alerts** (prevent surprises).
- **Per-job cost attribution** from system tables (know what each pipeline costs).
- **Serverless + scheduled** (pay per use, no idle clusters) and **incremental** processing
  (only new data each run — see `docs/streaming.md`) — the biggest steady-state cost lever.
