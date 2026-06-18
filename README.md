# MBTA On-Time Lakehouse

> **Is the train late, and why?** — an end-to-end lakehouse that ingests the MBTA's live
> transit feed, measures **on-time performance (OTP)**, and surfaces *where delays originate
> and cascade* — built on **GCP + Databricks**, fully reproducible from code.

🚧 **Status: Phase 0 (foundations).** Building in the open. See [roadmap](#roadmap).

---

## The problem

A transit-operations team needs to know not just *that* trains are late, but *where lateness
starts and how it propagates* across routes and stops — so they know where to intervene.
This project answers that from the MBTA's public real-time feed.

- **Stakeholder:** MBTA ops manager
- **Metric:** on-time performance (%) by route / stop / hour
- **Cadence:** near-real-time + daily rollups

## Architecture

```
GTFS-Realtime (stream) ─► GCP poller ─► Pub/Sub ─┐
                                                 ├─► GCS (bronze, raw)
GTFS static (batch) ─────────────────────────────┘        │
                                                          ▼
                              Databricks: Spark + Delta
                        bronze → silver (clean, RT⋈schedule, dedup, lateness)
                                                          │
                                                          ▼
                              gold (OTP marts: route/stop/hour)
                                                          │
                                                          ▼
                      Databricks SQL dashboard   ◄─ Unity Catalog (governance)
```

> Note: the Databricks workspace and its Unity Catalog managed storage are AWS-hosted
> (managed plumbing we don't operate directly). GCP owns ingestion, streaming, and raw
> object storage; Databricks owns compute, the medallion, governance, and serving.

## Stack

| Concern | Choice | Rationale |
|---|---|---|
| Storage | **Google Cloud Storage (GCS)** | raw RT object landing |
| Processing | **Spark on Databricks**, **Delta** | lakehouse core |
| Table layout | **Liquid Clustering** | avoids Hive over-partitioning |
| Governance | **Unity Catalog** | lineage, access control, audit |
| Orchestration | **Lakeflow Declarative Pipelines** | platform-native, preserves lineage |
| Streaming | **GCP Pub/Sub** | streaming + batch on one dataset |
| Compute | **job / serverless clusters** | auto-terminate; no 24/7 cost |
| IaC | **Terraform** (GCP + Databricks providers) | everything reproducible |
| CI/CD | **GitHub Actions** | tests + DQ on PR, plan→apply gating |

## Engineering principles

- **CLI-first / reproducible from code** — the entire platform (infra, pipelines, jobs)
  deploys from a terminal. Tool versions pinned via [`mise`](mise.toml); Python deps via `uv`.
- **Idempotent** pipelines (re-run / backfill safe).
- **Data-quality gates** before data is exposed.
- **Cost discipline** — runs under a strict budget; infra torn down between sessions; run
  cost reported.

## Quickstart (reproduce)

```bash
mise install          # provision pinned gcloud / terraform / databricks / python
uv sync               # install Python deps into .venv
cp .env.example .env  # then fill in MBTA_API_KEY etc. (never committed)
uv run pytest         # run the test suite
```

> Secrets are loaded from `.env` locally and from GCP Secret Manager / GitHub Actions
> secrets in CI. **No credentials are ever committed.**

## Roadmap

- **Phase 0** — foundations: repo, toolchain, first GTFS-RT byte landed in GCS ← *here*
- **Phase 1** — demoable batch slice: medallion bronze→silver→gold, real OTP numbers
- **Phase 2** — streaming (Pub/Sub → Structured Streaming/Lakeflow) + production rigor
- **Phase 3** — CI/CD, dashboard, architecture write-up, "hard decisions" doc

## Data source

MBTA V3 API / GTFS-Realtime — https://www.mbta.com/developers — public, free (API key).

---

*A data-engineering portfolio project. Built with Claude Code, operated CLI-first.*
