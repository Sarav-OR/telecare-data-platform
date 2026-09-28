# Day 3 – Orchestration with Azure Data Factory

Goal: the platform runs **by itself**. A scheduled ADF pipeline picks the next business date,
copies that day's deliveries from all 5 sources into `landing`, starts the Databricks job, waits
for it, and moves the batch clock forward. dbt Cloud builds the marts after it, guarded by a
blocking freshness check. Business dates 16–22 Sep are processed one after the other.

```
                     ┌──────────────────── pl_master_daily (scheduled, concurrency 1) ────────────────────┐
ctl.batch_state ───► │ lookup next business date ──► pl_run_business_date(run_date)                       │
                     │   ├─ pl_copy_file_feeds   vendor-drop ─► landing   (ctl.file_feed, missing = FAIL)  │
                     │   ├─ pl_extract_ehr       Azure SQL ─► Parquet     (ctl.sql_extract watermarks)     │
                     │   ├─ pl_copy_partner_api  REST ($.next paging) ─► JSON   (Mondays only)            │
                     │   ├─ pl_run_databricks_job  run-now + poll every 60 s, fail if not SUCCESS         │
                     │   └─ usp_complete_batch   clock +1 day  │ on any failure: ctl.run_log + rethrow    │
                     └──────────────────────────────────────────────────────────────────────────────────┘
dbt Cloud (scheduled 45 min later): dbt source freshness (blocking) ─► dbt build
```

Design choices you should be able to explain:

| Choice | Why | Real-world equivalent |
|---|---|---|
| **Metadata-driven** (control tables in `ctl`) | A new feed or table = one row, not a new pipeline | Standard "framework" pattern in most ADF shops |
| **Batch clock** (`ctl.batch_state`) instead of the trigger time | A failed day is simply re-run; the clock only moves on success and only by one day | Business-date / processing-date control |
| **Watermark per table**, moved *after* the copy | A failed copy never skips data; a re-run re-reads the same window | Classic incremental extract |
| **Managed identity everywhere** | No passwords or keys in ADF or Git | Required by most security teams |
| **Fail on missing delivery** | A late vendor file is an incident, not silently "0 rows" | Late-arrival alerting / SLA |
| **try / catch / rethrow** | Failures are logged in `ctl.run_log` *and* the pipeline still shows Failed (so alerts fire) | ADF error-handling pattern |
| **Hourly trigger = one business day** | Accelerated clock so 7 days run in an afternoon | Daily trigger after source cut-off |

Cost: each business date = one Databricks job run (~15–20 min single node) + a short dbt build on
the serverless warehouse. Seven dates ≈ a few CHF in total.

---

## D0. Get the new code (5 min)

The new files are already in your folder (`adf/`, `sql/ctl/`, two scripts in `data_generator/`,
small Databricks changes). Commit them:

```powershell
cd E:\SaravWorkspace\Projects\telecare-data-platform
git pull
git add -A
git commit -m "Day 3: ADF pipelines, control tables, API publisher; ingestion type-drift recovery"
git push
```

## D1. Control tables and ADF access in Azure SQL (5 min)

**Why:** ADF needs a place to read *what* to load and to write *how far* it got.
**Who (live):** data engineer writes the scripts; a DBA or the platform team deploys them.

```powershell
python data_generator/run_sql_script.py --server sql-telecare-dev-chn.database.windows.net --database sqldb-telecare-ehr --file sql/ctl/03_control_tables.sql --file sql/ehr/02_grant_adf_read.sql
```

✅ `ok  sql/ctl/03_control_tables.sql` and `ok  sql/ehr/02_grant_adf_read.sql`.
Check in the Query editor: `SELECT * FROM ctl.batch_state; SELECT * FROM ctl.file_feed; SELECT * FROM ctl.sql_extract;`

## D2. Let the EHR "move forward" to 22 Sep (2 min)

**Why:** the clinical system keeps changing; our extracts must find the new rows.

```powershell
python data_generator/load_ehr_to_sql.py --server sql-telecare-dev-chn.database.windows.net --database sqldb-telecare-ehr --data ./work/telecare/ehr --as-of "2026-09-22 23:59:59"
```

✅ Every table shows `since=2026-09-15 …` and a few thousand rows (only the changes).

> Design note: a watermark extract only sees the **current** state of a row. If a row changed
> twice between two extracts, the middle version is never seen. That is the known limit of
> watermark loads; true change data capture (CDC / change tracking) captures every version.

## D3. The partner REST API (static website) (10 min)

**Why:** the CRM's partner directory is only available as a paginated REST API.

1. Portal → `sttelecaredevchn` → **Data management → Static website** → **Enabled**,
   index document `index.html` → **Save**. Copy the **Primary endpoint**
   (e.g. `https://sttelecaredevchn.z1.web.core.windows.net/`).
2. Publish the API pages (rewrites every page's `next` link to your endpoint):
   ```powershell
   python data_generator/publish_api_site.py --account sttelecaredevchn --source ./work/telecare/api --base-url https://sttelecaredevchn.<zone>.web.core.windows.net/api
   ```
3. Open in a browser: `<endpoint>/api/partners/2026-09-21/page-0001.json` → JSON with `data` and `next`.

## D4. Databricks: let ADF start the job, deploy the new code (15 min)

**Why:** ADF calls the Jobs API with its **own managed identity**. Databricks must know that
identity, and it may only *run* the job (least privilege), not change it.
**Who (live):** security/platform admin adds the identity; the permission itself is code (bundle).

1. Portal → `adf-telecare-dev-chn` → **Properties** → copy **Managed identity application ID**.
2. Databricks → your name → **Settings → Identity and access → Service principals → Manage →
   Add service principal → Add new** → *Microsoft Entra ID managed* → paste the application ID,
   name `adf-telecare-dev-chn` → **Add**.
3. In `databricks/resources/orchestrator_access.yml` replace `<ADF_MI_APPLICATION_ID>` with the ID.
4. Deploy (new code: type-drift recovery, partner API contract, the permission):
   ```powershell
   cd databricks
   databricks bundle deploy -t dev --profile DEFAULT
   cd ..
   ```
5. Jobs → your job → copy the **Job ID** (number in the URL / job details). Check **Permissions**:
   `adf-telecare-dev-chn` → *Can Manage Run*.

## D5. Connect ADF to Git and load the pipelines (15 min)

**Why:** pipelines are code. Git gives history, reviews, and the ARM templates for prod (Day 4).
**Who (live):** data engineer; the platform team sets up the Git link once.

1. **Role check**: `sttelecaredevchn` → Access control (IAM) → Role assignments: `adf-telecare-dev-chn`
   must have **Storage Blob Data Contributor** (add it if missing).
2. ADF Studio → **Manage → Git configuration → Configure**:

   | Setting | Value |
   |---|---|
   | Repository type | GitHub → authorise |
   | Repository owner / name | `Sarav-OR` / `telecare-data-platform` |
   | Collaboration branch | `main` |
   | Publish branch | `adf_publish` |
   | Root folder | `/adf/` |
   | Import existing resources | **off** (the factory is empty) |

3. **Author** tab now shows 3 linked services, 5 datasets, 6 pipelines, 1 trigger.
4. Edit two values (then **Save**; in Git mode Save = commit):
   - Linked service `ls_rest_partner_api` → URL: replace `<zone>` with yours → **Test connection**.
   - Pipeline `pl_run_databricks_job` → Parameters → `job_id` default = your Job ID.
5. **Test connection** on `ls_adls_lake` and `ls_sql_ehr` (both managed identity) → green.
6. **Publish** (top bar). This deploys to the live factory and writes ARM templates to `adf_publish`.

## D6. First business date by hand (≈ 25 min, mostly waiting)

**Why:** always run manually once and check every step before switching on a schedule.

1. Author → `pl_master_daily` → **Add trigger → Trigger now**.
2. Monitor → Pipeline runs → open the run → you see `pl_run_business_date` for **2026-09-16**
   and its children:
   - `pl_copy_file_feeds`: 3 daily feeds (Wednesday → no weekly snapshots)
   - `pl_extract_ehr`: 8 tables, a few hundred to a few thousand rows each
   - `pl_copy_partner_api`: skipped (not a Monday)
   - `pl_run_databricks_job`: `until_run_finished` loops once per minute (~15–20 min)
3. ✅ Checks (SQL Query editor):
   ```sql
   SELECT * FROM ctl.batch_state;                      -- last_completed_date = 2026-09-16
   SELECT table_name, watermark_value, last_row_count FROM ctl.sql_extract;   -- 2026-09-17 00:00:00
   SELECT * FROM ctl.run_log ORDER BY log_id DESC;     -- STARTED, SUCCEEDED
   ```
   Databricks SQL editor – the ADF run id travels into the DQ metrics:
   ```sql
   SELECT orchestrator_run_id, dataset, SUM(total_rows) rows_in
   FROM telecare_dev.ops.dq_rule_results GROUP BY ALL ORDER BY 1 DESC LIMIT 20;
   ```
4. Run `Trigger now` once more → processes **2026-09-17**. Two good days = ready for automation.

## D7. Alerts (5 min)

ADF → **Monitor → Alerts & metrics → New alert rule**: name `telecare-pipeline-failed`,
severity 1, criteria **Failed pipeline runs metrics** > 0 (pipeline `pl_master_daily`),
action group: email to you. (Databricks job failures already email you from the bundle.)

## D8. Switch on the automation (5 min)

1. ADF → Author → trigger `tr_batch_hourly` → **Start trigger** → **Publish**.
   It runs at hh:05 every hour → 18, 19, 20, **21 (Monday: weekly snapshots + partner API)**, 22 Sep.
   After 22 Sep the master finds no new date and does nothing.
2. dbt Cloud → job `telecare_build` → Settings:
   - Commands: `dbt source freshness` then `dbt build` (freshness is now a **blocking gate**)
   - Untick **Run source freshness** (it is a command now)
   - Schedule: **on**, cron `50 * * * *` (45 min after ADF starts, when silver is fresh)

## D9. Verify end-to-end (after the last date, ~5–6 hours later)

- `ctl.batch_state.last_completed_date = 2026-09-22`, `ctl.run_log` shows 7 SUCCEEDED dates
- ADF Monitor: 7 green master runs (later ones "no work")
- dbt Cloud: green `telecare_build` runs, freshness PASS
- Marts contain encounters up to 22 Sep:
  `SELECT MAX(date_key) FROM telecare_dev.marts.fact_encounter;`
- Then **stop the trigger** (and the dbt schedule) to avoid idle runs.

📸 Screenshots: ADF master run with children, the Databricks polling loop, `ctl.run_log`,
the alert rule, dbt run with freshness PASS.
