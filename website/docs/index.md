# MBTA On-Time Lakehouse

**Is the MBTA late, and where? A live, self-managing data lakehouse that answers it — and runs,
heals, and improves itself.**

Every couple of minutes, this system captures Boston's live transit feed, compares actual arrivals
to the timetable, and publishes an **on-time-performance (OTP)** scoreboard — by route, by hour, by
stop. The whole pipeline runs unattended in the cloud. When something breaks, it retries or files a
ticket on its own. Each night an agent reads the results, writes a plain-English insight, and opens
a pull request proposing improvements.

It's built on **Databricks + Google Cloud**, entirely from code, for ~$0 — a deep, end-to-end
demonstration of modern data engineering.

## What it does

- **Ingests** MBTA GTFS-Realtime feeds (vehicle positions, trip updates) every 2 minutes.
- **Computes** lateness per stop by reconciling real-time arrivals against the schedule — handling
  the genuinely hard parts (timezones, after-midnight service).
- **Publishes** OTP marts: *the scorecard by route*, *when it degrades by hour*, and *where delays
  concentrate by stop* — plus an AI/BI dashboard.
- **Operates itself** — schedules for every step, automatic recovery, and an agentic layer that
  generates insights and proposals.

## Why it's different

- **Self-managing (agentic).** A nightly *Dreamer* writes insights and opens CI-gated pull
  requests; a *Monitor* heals failures. Safe fixes are automatic; consequential changes are
  proposed for a human to approve.
- **Everything is code.** The cloud is Terraform; the Databricks job is an Asset Bundle; the
  transform logic is a tested wheel the notebooks import. CI deploys infra with **no stored keys**.
- **Honest engineering.** Tested logic (including the after-midnight edge case), dev→prod
  discipline, data-quality gates — the kind of rigor that survives interview scrutiny.

## Explore

- **[How it works](how-it-works.md)** — the pipeline, in plain English.
- **[Under the hood](under-the-hood.md)** — the self-managing layer, infra-as-code, and testing — with diagrams.
- **[Showcase](showcase.md)** — what the system produces.
- **[Roadmap](roadmap.md)** — what's done and what's next.
- **[Getting started](getting-started.md)** — reproduce it yourself.

---

*Built by [João Blasques](https://github.com/joaoblasques) — AI-Enabled Data Engineer.*
