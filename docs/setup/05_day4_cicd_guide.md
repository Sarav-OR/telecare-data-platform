# Day 4 – CI/CD with GitHub Actions and branch protection

Goal: **no change reaches `main` without automated proof that it works**, and Databricks code is
deployed by a pipeline, not from a laptop.

```
feature branch ──► pull request ──► checks (all must be green) ──────────────► merge to main
                                     ├─ ci / lint-and-unit-tests   (ruff, 58 unit tests, ADF JSON, wheel)
                                     ├─ ci / end-to-end            (generator → Spark → DuckDB → dbt build
                                     │                              + incremental == full refresh)
                                     └─ dbt Cloud telecare_ci      (changed models on Databricks)
main ──► deploy-databricks-dev  (bundle validate + deploy -t dev)
```

| Workflow | When | What it proves |
|---|---|---|
| `ci.yml` → `lint-and-unit-tests` | every PR and push to main | code style, the DQ engine and transforms (local Spark), ADF definitions parse, the wheel builds |
| `ci.yml` → `end-to-end` | after the first job | the **whole chain** runs on 30 days of fresh synthetic data: 16 feeds with DQ, then all 78 dbt models and 169 tests on DuckDB, then incremental = full refresh |
| dbt Cloud `telecare_ci` | every PR (already set up) | the changed dbt models build on the real Databricks warehouse |
| `deploy-databricks-dev.yml` | merge to main touching `databricks/` | the bundle deploys without a laptop |

Why the end-to-end test runs on DuckDB and local Spark: it needs **no cloud resources, costs
nothing, and takes ~6 minutes**, so it can run on every PR. The same Python package and the same
dbt project run on Databricks; only the I/O layer and the dbt adapter differ (cross-database macros).

---

## E0. Put the new files on a branch (5 min)

New files: `.github/workflows/ci.yml`, `.github/workflows/deploy-databricks-dev.yml`, `ruff.toml`,
this guide, a one-line lint fix in `data_generator/build_landing.py`, README badge.

```powershell
cd E:\SaravWorkspace\Projects\telecare-data-platform
git checkout main
git pull
git checkout -b feature/day4-cicd
git add -A
git commit -m "Day 4: GitHub Actions CI (lint, unit, end-to-end) and Databricks dev deployment"
git push -u origin feature/day4-cicd
```

## E1. GitHub environment for the deployment secret (5 min)

**Why:** the deploy workflow needs to log in to Databricks. Secrets live in GitHub, never in Git.
**Live project:** a **service principal** with OAuth (M2M) or GitHub OIDC federation, no personal token.

GitHub → repo → **Settings → Environments → New environment** `dev`:
- **Environment variables** → `DATABRICKS_HOST` = `https://adb-7405609552599914.14.azuredatabricks.net`
- **Environment secrets** → `DATABRICKS_TOKEN` = a Databricks PAT (new one, comment `github-actions-dev`, 30 days)

## E2. Open the pull request and watch the checks (≈ 10 min, mostly waiting)

GitHub → **Compare & pull request** → base `main` ← `feature/day4-cicd` → **Create pull request**.

The PR page shows three checks:
- `ci / lint-and-unit-tests` (~3 min)
- `ci / end-to-end` (~6–8 min) – open **Details** to see each numbered step
- `dbt Cloud — telecare_ci` (no dbt change → nothing to build → green)

Do **not** merge yet.

## E3. Branch protection (5 min)

**Why:** CI that can be ignored is decoration. Protection makes it mandatory.

GitHub → **Settings → Branches → Add branch ruleset** (or *Add classic branch protection rule*), branch `main`:
- ✅ Require a pull request before merging (approvals: 0 – you are the only developer)
- ✅ Require status checks to pass: `lint-and-unit-tests`, `end-to-end`, `dbt Cloud — telecare_ci`
- ✅ Block force pushes
- Bypass: allow **repository admins** – for emergencies only, and documented as a known exception

> ADF note: ADF's Git mode saves to `main` directly. With protection on, create a **working
> branch in ADF** (branch dropdown → *New branch*), save there, open a PR, merge, then Publish
> from `main`. That is exactly how ADF teams work.

## E4. Merge and watch the deployment (5 min)

Merge the PR (all checks green) → **Actions** tab → `deploy-databricks-dev` starts (because files
under `databricks/`? – not in this PR; run it once by hand: **Actions → deploy-databricks-dev →
Run workflow**). ✅ Both steps green: `bundle validate`, `bundle deploy`.

From now on nobody needs `databricks bundle deploy` on a laptop: merge = deploy to dev.

## E5. What production would add (documented, not provisioned – cost)

| Piece | How |
|---|---|
| `deploy-prd` job | same steps with `-t prd`, `environment: prd` with **required reviewers** (approval gate) |
| Identity | service principal `sp-telecare-cicd` (OAuth M2M) + `run_as` in the prd target |
| Infra | `rg-telecare-prd-chn`, `sttelecareprdchn`, catalog `telecare_prd`, separate ADF – via Bicep/Terraform |
| ADF | ARM templates from `adf_publish` deployed with prd parameter file (linked service URLs, job id) |
| dbt Cloud | Production environment `DBT_SOURCE_CATALOG=telecare_prd`, service-principal credentials |

📸 Screenshots: the PR with 3 green checks, the `end-to-end` step list, the branch rule, a green deployment.
