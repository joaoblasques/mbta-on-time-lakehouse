# Architecture

> Living document. Diagram + "hard decisions" write-up land in Phase 3.

## Medallion flow

- **Bronze** — raw GTFS-Realtime snapshots (via GCP poller → Pub/Sub → GCS) + raw GTFS
  static, landed as-received. (Static GTFS currently lands in a Databricks UC volume; RT
  lands in GCS.)
- **Silver** — cleaned, deduplicated, real-time positions joined to scheduled stop times;
  lateness computed per vehicle/trip/stop.
- **Gold** — OTP marts aggregated by route / stop / hour for serving.

## Decisions log (interview ammo — fill as we go)

- [ ] Liquid Clustering vs Hive partitioning — why
- [ ] Lakeflow Declarative Pipelines vs dbt on Databricks — why
- [ ] Dedup strategy for out-of-order / duplicate GTFS-RT pings
- [ ] Streaming (Pub/Sub) vs micro-batch tradeoffs
- [ ] Terraform state backend (GCS) + cost guardrails

### 2026-06-18 — RT ingestion: GCP Pub/Sub instead of AWS Kinesis

**Decision:** the GTFS-Realtime poller (the "actuals" that unlock lateness) runs on
**GCP** and publishes to **Pub/Sub**, rather than AWS Kinesis as originally documented.
Databricks (on AWS) consumes from there into bronze.

**Why:** GCP $300 free-tier credits keep this inside the <$20 budget; Pub/Sub signup is
frictionless via Gmail. Accepted trade-off: a third cloud and a Terraform GCP provider.

**Consequences:** Terraform spans **GCP + Databricks** providers. Streaming story is
Pub/Sub, not Kinesis. GCP project + Pub/Sub teardown added to the between-sessions checklist.
Supersedes the "Streaming: Kinesis" line in CLAUDE.md (updated same commit).

**Extended to a full reframe (same day):** the project headline is now **GCP + Databricks**
(was AWS + Databricks). GCP owns ingestion / streaming / raw object storage (poller,
Pub/Sub, GCS); Databricks owns compute / medallion / governance / serving. We **keep the
existing Databricks workspace** even though its Unity Catalog managed storage is AWS-hosted
(S3) — that's managed plumbing we don't operate, not part of an AWS-native design, so no
rebuild and today's verified bronze/silver tables stay. `awscli` dropped from `mise.toml`
in favour of `gcloud`; `boto3` replaced by `google-cloud-pubsub` / `google-cloud-storage`.
