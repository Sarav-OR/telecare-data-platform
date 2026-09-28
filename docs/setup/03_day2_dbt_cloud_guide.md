# Day 2 (part 2) – dbt Cloud: connect, build the vault and the Kimball marts

Prerequisite: the Databricks job of the Day 2 guide finished and `telecare_dev.silver` is filled.
The dbt project lives in the repo folder `dbt/` (78 models, 12 seeds, 168 data tests, 1 unit test).
It was built and tested locally on the same data (DuckDB) – all tests pass.

Plan limits (Developer plan): one project, one developer seat, 3,000 successful model builds
per month, no API. A full build = 78 models, so keep scheduled runs to ~4 per day.

---

## C1. Personal access token for dbt Cloud

Databricks → Settings → **Developer → Access tokens → Generate new token**
- Comment `dbt-cloud-dev`, lifetime **30 days**
- Copy it once; store it in Key Vault `kv-telecare-dev-chn` as secret `dbt-cloud-databricks-pat`
  (the vault is the system of record; dbt Cloud keeps its own encrypted copy).

SQL warehouse → `wh-telecare-dev` → **Connection details**: copy **Server hostname** and **HTTP path**.

## C2. Project and connection

dbt Cloud → **Account settings → Projects → New project**
| Setting | Value |
|---|---|
| Project name | `telecare` |
| Connection | **Databricks** (adapter *dbt-databricks*) |
| Server hostname | `adb-7405609552599914.14.azuredatabricks.net` |
| HTTP path | from C1 |
| Catalog | `telecare_dev` |

**Repository**: GitHub → authorise the dbt Cloud GitHub app → `Sarav-OR/telecare-data-platform`.
Then **Project settings → Project subdirectory** = `dbt` (the project is not at the repo root).

## C3. Development credentials

Your profile → **Credentials → telecare**
| Setting | Value |
|---|---|
| Auth method | Token |
| Token | the PAT from C1 |
| Schema | `dbt_sarav` (your models go to `dbt_sarav_staging`, `dbt_sarav_raw_vault`, `dbt_sarav_marts` …) |
| Threads | 4 |

## C4. Environment variables

**Deploy → Environments → Environment variables → Add variable**
| Key | Project default | Development | Production |
|---|---|---|---|
| `DBT_SOURCE_CATALOG` | `telecare_dev` | `telecare_dev` | `telecare_dev` *(→ `telecare_prd` on Day 4)* |

## C5. First build in the Studio IDE

Open **Studio (IDE)** on branch `main`, create a branch `feature/dbt-first-build`, then in the
command bar:

```
dbt deps          # no packages – just checks the setup
dbt seed          # 12 reference tables -> dbt_sarav_reference
dbt build         # 78 models + 168 tests + 1 unit test (~5-8 min on 2X-Small)
```

Expected: `PASS=259 WARN=0 ERROR=0` (locally measured; a few WARN are fine).
If a test fails, open the Details tab: the compiled SQL returns the failing rows and shows where to look.

Useful checks in the Databricks SQL editor:
```sql
-- the vault-to-Kimball reconciliation passed if these match
SELECT COUNT(*) FROM telecare_dev.dbt_sarav_raw_vault.hub_encounter;
SELECT COUNT(*) FROM telecare_dev.dbt_sarav_marts.fact_encounter;

-- business questions the marts answer
SELECT d.service_line_name, COUNT(*) encounters, ROUND(AVG(f.is_resolved_remotely::int), 3) remote_rate
FROM telecare_dev.dbt_sarav_marts.fact_encounter f
JOIN telecare_dev.dbt_sarav_marts.dim_service_line d ON d.service_line_key = f.service_line_key
WHERE NOT f.is_deleted GROUP BY 1 ORDER BY 2 DESC;

SELECT q.queue_name, ROUND(AVG(f.is_answered_within_sl::int), 3) service_level, ROUND(AVG(f.is_abandoned::int), 3) abandon_rate
FROM telecare_dev.dbt_sarav_marts.fact_contact f
JOIN telecare_dev.dbt_sarav_marts.dim_queue q ON q.queue_key = f.queue_key
WHERE f.contact_system = 'TEL' GROUP BY 1 ORDER BY 2;
```

Commit the branch, open a pull request on GitHub and merge it (this also tests the Git link).

## C6. Production environment and scheduled job

**Deploy → Environments → Create environment**
| Setting | Value |
|---|---|
| Name | `Production` |
| Type | Deployment → **Production** |
| dbt version | Latest |
| Connection | the Databricks connection |
| Deployment credentials | token = PAT, schema = `analytics` (ignored – the project uses layer schemas) |

**Deploy → Jobs → Create job → Deploy job** – `telecare_build`
| Setting | Value |
|---|---|
| Environment | Production |
| Target name | **`prod`** (switches schemas to `staging`, `raw_vault`, `marts` …) |
| Commands | `dbt source freshness` then `dbt build` |
| Generate docs on run | ✅ |
| Schedule | cron `30 1,7,13,19 * * *` (4 runs/day, 30 min after ADF) – leave **off** until Day 3 |

`dbt source freshness` is the **freshness gate**: on the free plan ADF cannot trigger dbt
Cloud, so the job runs on a schedule and refuses to build marts on stale silver data
(error after 26 h for hourly feeds). Run it once manually now (**Run now**).

## C7. CI job for pull requests

**Deploy → Jobs → Create job → Continuous integration job** – `telecare_ci`
| Setting | Value |
|---|---|
| Environment | Production (or a separate *CI* environment if your plan allows) |
| Triggered by | Pull requests |
| Commands | `dbt build --select state:modified+` |
| Defer to | Production |

From now on every PR builds only the changed models (and everything downstream) in a
temporary `dbt_cloud_pr_…` schema and reports the result back to GitHub.

## C8. Screenshots for the README
- Lineage graph (Studio → Lineage) for `fact_encounter` – shows silver → vault → Kimball
- Job run with the test summary
- Docs site → `fact_encounter` page
