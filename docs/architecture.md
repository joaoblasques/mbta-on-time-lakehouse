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
