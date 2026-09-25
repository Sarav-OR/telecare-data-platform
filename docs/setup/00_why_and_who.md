# Why each setup step exists – and who does it in a real project

Every step of the setup guides answers three questions:
**Why** do we do it here? **How** is it handled in a live project? **Who** owns it?

## Roles in a typical data-platform team

| Role | Owns |
|---|---|
| **Cloud / platform engineer** (sometimes "DevOps" or "cloud admin") | Subscriptions, resource groups, networking, Key Vault, storage accounts, Databricks workspaces, Unity Catalog metastore, cluster policies – usually as Infrastructure-as-Code (Terraform / Bicep) |
| **Identity / security admin** | Entra ID users, groups, service principals, role assignments, PII access groups |
| **Data engineer** (you) | Pipelines (ADF), ingestion code (Databricks), transformations (dbt), tests, CI/CD pipelines for the data code |
| **Analytics engineer / BI** | Marts, semantic models, dashboards |
| **Source-system owners** (EHR team, telephony vendor, CRM team) | Delivering the data, schemas, data contracts |

In this project **you play all roles**. In the interview you can say which role each step belongs to.

---

## Day 1 – Azure foundation (01_azure_setup_guide.md)

| Step | Why here | Live project | Who |
|---|---|---|---|
| Resource group, storage, Key Vault, SQL, ADF, Databricks | The platform must exist before code can run | Created once per environment by **Terraform/Bicep** in an infra pipeline, reviewed via pull request; never clicked in the portal in prod | Platform engineer |
| Budget alert | Cost control on a personal subscription | FinOps budgets and tags per cost centre, monthly reports | Platform engineer / FinOps |
| Access connector, storage credential, external locations | Databricks reads storage with a managed identity – no keys | Same, defined in Terraform (Databricks provider) | Platform engineer |
| Role assignments (ADF → storage, Key Vault) | Least privilege between services | Same, as code; access reviews quarterly | Identity / security admin |
| GitHub repository | Single source of truth for all code | Organisation repo with branch protection, required reviews, CODEOWNERS | Team lead / platform |

---

## Day 2 – Part 1: Databricks (02_day2_databricks_guide.md)

| Step | Why here | Live project | Who |
|---|---|---|---|
| **B1 Local tools** – Python, Azure CLI, Databricks CLI, ODBC driver, virtual environment | Your laptop is the developer workstation: it runs the scripts, talks to Azure, deploys to **dev** | Same for every developer (often a standard dev container / VDI image). **Prod is never deployed from a laptop** – only from CI/CD | Each developer (image provided by platform team) |
| B1 `.venv` + `requirements-dev.txt` | Exact, isolated library versions → "works on my machine" = works in CI | Same; versions pinned, updated via pull requests (e.g. Dependabot) | Data engineer |
| B1 `az login`, `databricks auth login` | Personal identity (browser/OAuth) – no passwords or tokens in files | Developers use their own identity in dev; **pipelines and CI use service principals** | Developer (own login), security admin (service principals) |
| **B2 Push to GitHub** | dbt Cloud and later CI/CD read the code from Git | Feature branch → pull request → review → merge to main; main is protected | Data engineer + reviewer |
| **B3 Generate data** | We have no real hospital – the generator plays the source systems | Doesn't exist: real systems deliver data. Test data comes from anonymised copies or synthetic generators | (Source-system owners) |
| **B4 Storage data role for you** | Owner can manage the account but not read/write data | Developers get data access in **dev only**; prod data access via approved groups | Identity / security admin |
| **B5 Load the clinical DB** | Simulates the hospital EHR being live | Doesn't exist: the EHR team runs the database; we only get read access | EHR team (source owner) |
| **B6 Upload files** | Simulates vendors delivering files + one-off bootstrap of `landing` | Vendors push to `vendor-drop` (SFTP/API); **ADF** writes `landing` – nobody uploads by hand | Vendors; ADF (data engineer) |
| **B7 Cluster policy** | Guarantees small, auto-terminating compute → cost control | Policies defined centrally, assigned per team; users can't create unrestricted clusters | Platform engineer |
| **B8 Deploy the bundle** | Job definition as code (Databricks Asset Bundle) → reproducible | `deploy -t dev` by the developer; `deploy -t prd` **only by the CI/CD pipeline** after approval | Data engineer (dev), CI/CD (prd) |
| **B8b PII group** | Only authorised identities see the date of birth (mask on the vault; bronze/silver are a restricted zone) | Access to PII requested and approved (DPO / data owner); only pipeline service principals are members | Security admin + data protection officer |
| **B9 Run the job** | First real load, validates the whole ingestion | Triggered by ADF on a schedule; manual runs only for backfills / incidents | ADF (orchestration), on-call engineer |
| **B10 Verify** | Evidence that silver is complete and correct | Automated: DQ metrics, reconciliation tests, alerts; dashboards for data quality | Data engineer |

---

## Day 2 – Part 2: dbt Cloud (03_day2_dbt_cloud_guide.md)

| Step | Why here | Live project | Who |
|---|---|---|---|
| C1 Token for dbt Cloud | dbt Cloud needs credentials for Databricks | **Service principal with OAuth (M2M)**, token stored in a vault and rotated; personal tokens only in dev | Security admin / platform |
| C2 Project, connection, Git link | dbt Cloud runs the models from the repo | Same, one project per domain; connection managed by platform team | Analytics lead / platform |
| C3 Dev credentials, schema `dbt_<name>` | Each developer builds into their own schemas – no collisions | Same pattern for every developer | Each developer |
| C4 Environment variables | Same code, different catalogs per environment | Same; values managed per environment, never in code | Data / analytics engineer |
| C5 First build in the IDE | Proves models + tests work on real data | Developers build and test on their branch before opening a PR | Developer |
| C6 Production environment + job | Scheduled, reproducible prod builds | Triggered by the orchestrator (ADF/Airflow) via API after ingestion, or on a schedule with freshness gates | Data engineer; on-call for failures |
| C7 CI job on pull requests | Every change is built and tested before merge | Mandatory PR check; merge blocked if it fails | Team (enforced by branch protection) |
| C8 Screenshots | Portfolio evidence | Documentation lives in dbt Docs / data catalog | – |

---

## One-line summary for the interview

> "In this project I played all roles – platform, security and data engineer. In a real team
> the platform team provisions infrastructure as code, security manages identities and PII
> access, and data engineers own the pipelines, tests and CI/CD. Developers deploy only to dev
> with their own identity; production is deployed exclusively by pipelines running as service
> principals."
