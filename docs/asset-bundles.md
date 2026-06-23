# Databricks Asset Bundles (DABs)

## What this is

The medallion pipeline is deployed to Databricks as a **Databricks Asset Bundle** — the
Databricks-native infrastructure-as-code format. Instead of creating the job imperatively
(`databricks jobs create`/`submit` + a hand-maintained JSON), the job is **declared in YAML in
this repo** and deployed with one command. The bundle owns the job and the notebooks it runs.

```
databricks.yml                 # bundle name + targets (dev / prod) + workspace host
resources/medallion.job.yml    # the medallion job declared as code (3-task DAG, schedule)
databricks/notebooks/*.py      # the notebooks the bundle uploads + the job runs
```

## Why we migrated (the "why")

Before, the medallion job was created imperatively and described by an ad-hoc
`databricks/jobs/medallion_refresh.job.json` we submitted by hand. That works, but:

| Imperative (`jobs submit`) | Asset Bundle (DAB) |
|---|---|
| Job state lives **in the workspace**, not in git | Job is **defined in git**, reviewable in PRs |
| Notebooks uploaded manually / drift from repo | Bundle **uploads notebooks** on every deploy — no drift |
| One environment; copy-paste to make another | **Targets** (`dev`/`prod`) from the same source |
| "What's deployed?" = click around the UI | `databricks bundle summary` / the YAML is the source of truth |
| No clean teardown | `databricks bundle destroy` removes everything it made |

DABs are the **current standard** for shipping Databricks jobs/pipelines/dashboards as code —
the same reason we use Terraform for GCP. This makes the project reproducible and review-driven
end to end, and is a concrete, in-demand Databricks skill.

## Targets

- **`dev`** — `mode: development`. Resources get a `[dev <user>]` name prefix and schedules are
  **paused** automatically, so deploying can't disturb anything. Use for safe iteration.
- **`prod`** — `mode: production`. Clean names, schedule live as declared. The real deployment.

## Commands

```bash
databricks bundle validate -t prod -p mbta          # type-check the bundle
databricks bundle deploy   -t prod -p mbta          # upload notebooks + create/update the job
databricks bundle run medallion_refresh -t prod -p mbta   # trigger a run, wait, stream output
databricks bundle summary  -t prod -p mbta          # what's deployed (job URLs/ids)
databricks bundle destroy  -t prod -p mbta          # remove everything the bundle created
```

## Notes / decisions

- **Serverless:** the notebook tasks declare no cluster, so they run on serverless (the only
  option on Free Edition). No `job_clusters` block needed.
- **Notebooks as code:** each `.py` starts with `# Databricks notebook source`, so the bundle
  deploys them as notebooks (not arbitrary files) and `notebook_task` can run them.
- **Migration:** this bundle **replaces** the old imperative job + `databricks/jobs/
  medallion_refresh.job.json`. The failure-monitor's `MEDALLION_JOB_ID` was repointed to the
  bundle-managed job, and the old imperative job was deleted to avoid duplicate hourly runs.
- **Scope (now):** the medallion job. **Next:** bring the AI/BI dashboard and (eventually) a
  Lakeflow Declarative Pipeline under the same bundle.
```
