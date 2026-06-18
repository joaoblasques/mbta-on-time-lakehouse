# Architecture

> Living document. Diagram + "hard decisions" write-up land in Phase 3.

## Medallion flow

- **Bronze** — raw GTFS-Realtime snapshots + raw GTFS static, landed in S3 as-received.
- **Silver** — cleaned, deduplicated, real-time positions joined to scheduled stop times;
  lateness computed per vehicle/trip/stop.
- **Gold** — OTP marts aggregated by route / stop / hour for serving.

## Decisions log (interview ammo — fill as we go)

- [ ] Liquid Clustering vs Hive partitioning — why
- [ ] Lakeflow Declarative Pipelines vs dbt on Databricks — why
- [ ] Dedup strategy for out-of-order / duplicate GTFS-RT pings
- [ ] Streaming (Kinesis) vs micro-batch tradeoffs
- [ ] Terraform state backend (S3) + cost guardrails

### 2026-06-18 — RT ingestion: GCP Pub/Sub instead of AWS Kinesis

**Decision:** the GTFS-Realtime poller (the "actuals" that unlock lateness) runs on
**GCP** and publishes to **Pub/Sub**, rather than AWS Kinesis as originally documented.
Databricks (on AWS) consumes from there into bronze.

**Why:** GCP $300 free-tier credits keep this inside the <$20 budget; Pub/Sub signup is
frictionless via Gmail. Accepted trade-off: a third cloud and a Terraform GCP provider.

**Consequences:** Terraform now spans AWS + Databricks + GCP providers. Streaming story is
Pub/Sub, not Kinesis. GCP project + Pub/Sub teardown added to the between-sessions checklist.
Supersedes the "Streaming: Kinesis" line in CLAUDE.md (updated same commit).
