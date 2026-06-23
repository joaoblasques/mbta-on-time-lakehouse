# Getting started

The project is reproducible from the [repository](https://github.com/joaoblasques/mbta-on-time-lakehouse).
High-level — full details live in the repo's `README.md` and `docs/`.

## Prerequisites

- A **GCP** project (free credits are plenty) and a **Databricks** workspace (Free Edition works).
- **[mise](https://mise.jdx.dev/)** — pins the toolchain (`gcloud`, `terraform`, `databricks`, `uv`, Python).
- **[uv](https://docs.astral.sh/uv/)** for Python dependencies.

```bash
git clone https://github.com/joaoblasques/mbta-on-time-lakehouse
cd mbta-on-time-lakehouse
mise install            # installs the pinned tools
uv sync --all-groups    # Python deps (incl. pyspark for tests)
```

## Run the tests

```bash
uv run pytest -q        # unit + Spark integration tests (needs Java 17+)
```

## Provision the cloud (Terraform)

```bash
cd terraform
terraform init
terraform apply -var project_id=<your-gcp-project>
```

Provisions the GCS bucket, Cloud Run jobs (poller, copier, dreamer, monitor), schedulers, and
Secret Manager wiring. In CI this runs keyless via Workload Identity Federation.

## Deploy the Databricks job (Asset Bundle)

```bash
databricks bundle validate -t prod
databricks bundle deploy   -t prod      # builds the transforms wheel + deploys the medallion job
databricks bundle run medallion_refresh -t prod
```

## Pause / resume the autonomous loop

Everything runs on schedules. To stop incurring (tiny) cost:

```bash
gcloud scheduler jobs pause mbta-poller   --location us-east1
gcloud scheduler jobs pause mbta-copier   --location us-east1
gcloud scheduler jobs pause mbta-dreamer  --location us-east1
gcloud scheduler jobs pause mbta-monitor  --location us-east1
# resume with `gcloud scheduler jobs resume ...`
```

## Learn more

- **[How it works](how-it-works.md)** · **[Under the hood](under-the-hood.md)** · **[Showcase](showcase.md)**
- Repo docs: `docs/explainer.md` (plain English), `docs/architecture.md` (decisions),
  `docs/asset-bundles.md`, `docs/ci-cd.md`, `docs/testing.md`, `docs/streaming.md`.
