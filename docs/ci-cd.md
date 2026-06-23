# CI/CD

Two GitHub Actions workflows:

| Workflow | Trigger | Does |
|---|---|---|
| `ci.yml` | every PR/push | lint (ruff) + tests (pytest) + `terraform fmt/validate` + job-JSON checks |
| `terraform.yml` | infra PRs / merge to main | `terraform plan` on PR, **`terraform apply` on merge** — keyless via **WIF** |

## Keyless auth: Workload Identity Federation (WIF)

The terraform workflow needs to act on GCP, but **storing a service-account JSON key in GitHub is
a liability** (long-lived, leakable, manual rotation). Instead we use **Workload Identity
Federation** — short-lived, keyless, OIDC-based:

```
GitHub Actions ──(1) mint OIDC token (id-token: write)──► GitHub
      │                                                     token says: repo=joaoblasques/mbta-on-time-lakehouse
      └──(2) present token──► GCP STS ──(3) verify against the WIF provider──► (4) short-lived
                                          (issuer + attribute.repository == our repo)        access token
                                                                                  │
                                          (5) impersonate the CI service account ◄┘ → run terraform
```

1. The job requests an **OIDC token** from GitHub (`permissions: id-token: write`).
2. `google-github-actions/auth` sends it to GCP's Security Token Service.
3. GCP checks it against our **WIF provider**, whose **attribute condition** only accepts tokens
   where `assertion.repository == 'joaoblasques/mbta-on-time-lakehouse'` — so *only this repo* can
   authenticate. The issuer is pinned to `token.actions.githubusercontent.com`.
4. GCP returns a **short-lived** credential (minutes, auto-expiring).
5. That credential **impersonates the `mbta-ci` service account** (granted via
   `roles/iam.workloadIdentityUser` on a `principalSet` scoped to the repo) and runs terraform.

**No JSON key exists anywhere.** Nothing to leak or rotate.

## The flow

- **On a PR touching `terraform/**`** → `init` + `fmt -check` + `validate` + **`plan`**. The plan
  (in the check logs) is the review artifact: you see exactly what would change before merging.
- **On merge to `main`** → the same, then **`apply`**. Infra changes ship only through reviewed,
  merged PRs — GitOps for the GCP layer.

## The CI service account (`mbta-ci`) — least privilege

Granted only the admin roles terraform actually uses (not Owner/Editor):
`storage.admin` (incl. the GCS state bucket), `run.admin`, `cloudscheduler.admin`, `pubsub.admin`,
`artifactregistry.admin`, `secretmanager.admin`, `iam.serviceAccountAdmin`,
`serviceusage.serviceUsageAdmin`, `iam.serviceAccountUser`.

## Bootstrap (one-time, out-of-band)

The WIF pool/provider + `mbta-ci` SA + bindings were created with `gcloud` (chicken-and-egg: the
SA that runs terraform can't be created by that same terraform run). Recreate with:

```bash
gcloud iam workload-identity-pools create github-pool --location=global
gcloud iam workload-identity-pools providers create-oidc github-provider \
  --location=global --workload-identity-pool=github-pool \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository=='joaoblasques/mbta-on-time-lakehouse'"
gcloud iam service-accounts create mbta-ci
# grant the roles above, then bind impersonation:
gcloud iam service-accounts add-iam-policy-binding mbta-ci@<project>.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/github-pool/attribute.repository/joaoblasques/mbta-on-time-lakehouse"
```

Provider resource name (used in `terraform.yml`):
`projects/403428212023/locations/global/workloadIdentityPools/github-pool/providers/github-provider`.
