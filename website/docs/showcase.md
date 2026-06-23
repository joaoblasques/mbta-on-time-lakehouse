# Showcase

What the system actually produces.

## The OTP scoreboard (Gold)

Three marts answer three questions:

- **OTP by route** — *the scorecard.* Which routes run on time, and which chronically don't.
- **OTP by route × hour** — *when does it degrade?* Peak-hour vs off-peak performance.
- **OTP by stop** — *where do delays concentrate?* The specific stops dragging a route down.

All published to a Databricks **AI/BI dashboard** and refreshed hourly.

## A real nightly insight (written by the Dreamer)

The Dreamer reads the Gold marts and writes this kind of plain-English analysis itself, then opens
it as a pull request:

> **What's notable**
>
> - **Severe route-level delays** — a handful of routes are the only ones whose *median* arrival is
>   actually late; most hover around on-time or slightly early.
> - **Hot-spot corridors** — clusters of stops on specific streets show 10–11 min median lateness
>   with very low OTP — delays concentrate geographically, not evenly.
> - **Early-arrival pockets** — some school/park-area stops run several minutes *early*, a different
>   problem worth surfacing.
> - **System imbalance** — the system median is only slightly late, but the distribution is skewed:
>   a few bad corridors pull the average up while many stops are fine.
>
> **Proposed new marts**
>
> 1. A **Stop-Level Late/Early Index** ranking high-impact stops.
> 2. A **corridor delay heatmap** aggregating lateness by street.
> 3. A **route–stop synchronization score** to flag schedules needing adjustment.

Every insight ships as a **CI-gated pull request** with the verified findings appended as the
authoritative source — the LLM narrates, but the numbers are computed and checked.

## What this demonstrates

- A complete **medallion lakehouse** on Databricks, from live feed to business-ready marts.
- **Real-time + schedule reconciliation** with the genuinely hard temporal edge cases handled.
- An **agentic operations layer** — insights and self-healing — on top of the data platform.
- Production practices: **IaC, CI/CD, keyless auth, tested transforms, data-quality gates**.

> See [Under the hood](under-the-hood.md) for how it's built.
