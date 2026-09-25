# Day 2 – Load the sources, deploy and run the Databricks ingestion

Goal: at the end, `telecare_dev.silver` holds 83 days of validated data from all 5 source
systems (up to 15 Sep 2026), the clinical database is live in Azure SQL, and the
source deliveries sit in `vendor-drop` ready for ADF tomorrow.

Time: ~1.5 h of your work + ~25 min job run. Cloud cost: one single-node job run
(it terminates itself when done), plus a few minutes of the SQL warehouse.

```
work/telecare/vendor-drop ──upload──► ADLS vendor-drop        (ADF reads this from Day 3)
work/telecare/ehr/*.csv   ──loader──► Azure SQL ehr.*         (ADF reads this from Day 3)
work/bootstrap/landing    ──upload──► ADLS landing  ──► Databricks job ──► bronze ──► silver
       (landing as ADF would have produced it up to 15 Sep: a one-off bootstrap)
```

---

## B1. Local tools (once, Windows / PowerShell)

Check what is already installed – keep it if the version fits:

```powershell
py --list                       # need 3.11 or 3.12 (3.10 works too; avoid 3.13+ for now)
az --version                    # any 2.6x+
databricks --version            # must be v0.2xx or newer. "Version 0.18" = legacy CLI -> see below
git --version
Get-OdbcDriver | Where-Object Name -like "*SQL Server*" | Select-Object Name   # need "ODBC Driver 18 for SQL Server"
```

Install only what is missing:

```powershell
winget install Python.Python.3.11
winget install Microsoft.AzureCLI
winget install Databricks.DatabricksCLI
winget install Microsoft.msodbcsql.18      # or the MSI from Microsoft's "Download ODBC Driver for SQL Server" page
```

If `databricks --version` shows **0.18.x**, that is the old pip package `databricks-cli`; remove it
(`pip uninstall databricks-cli`) so the new CLI is found first. Close and reopen PowerShell after installs.

Project environment (isolated – nothing from earlier projects is touched):

```powershell
cd <folder where you unzipped>\telecare-data-platform
py -3.11 -m venv .venv
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned     # once, allows the activate script
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements-dev.txt

az login
databricks auth login --host https://adb-7405609552599914.14.azuredatabricks.net
```

Java and Spark are **not** needed on your laptop: everything Spark-related runs in Databricks.
(The local Spark tests and runner are for CI on GitHub's Linux machines.)

✅ `databricks current-user me` prints your user; `(.venv)` is shown in the prompt.

---

## B2. Push the repo to GitHub

```bash
git init -b main
git add .
git commit -m "Day 1: generator, EHR schema, Databricks ingestion framework"
git remote add origin https://github.com/Sarav-OR/telecare-data-platform.git
git push -u origin main
```

✅ The files are visible on GitHub, and there is **no** `work/` folder (it's in `.gitignore`).

---

## B3. Generate the data (local, ~1 min)

```bash
python data_generator/generate.py --out ./work/telecare
```

✅ The last line reads `done in … s`. `work/telecare/manifest.json` lists the counts.

---

## B4. Give yourself data access to the storage account

Portal → `sttelecaredevchn` → **Access control (IAM)** → Add role assignment →
**Storage Blob Data Contributor** → your user.
(Owner manages the resource but cannot read or write data – same gotcha as Key Vault.)
Wait ~5 minutes before B6.

---

## B5. Load the clinical system (Azure SQL) up to 15 Sep

```bash
cd data_generator
python load_ehr_to_sql.py --server sql-telecare-dev-chn.database.windows.net --database sqldb-telecare-ehr --data ../work/telecare/ehr --as-of "2026-09-15 23:59:59"
cd ..
```

The loader creates the `ehr` schema (script `sql/ehr/01_create_ehr_schema.sql`) and applies
every row version up to that moment. Expected (±1 %):

| table | rows |
|---|---|
| patient | ~28,900 |
| encounter | ~135,800 |
| encounter_diagnosis | ~171,600 |
| prescription / referral / sick_note | ~73,700 / ~56,400 / ~12,100 |

If it fails with a *login timeout*, your IP changed: server → Networking → add client IP.

✅ In the Query editor: `SELECT status, COUNT(*) FROM ehr.encounter GROUP BY status;`

---

## B6. Upload the source deliveries and the bootstrap landing zone

```bash
# 1. all 90 days of file deliveries -> vendor-drop (ADF picks from here tomorrow)
python data_generator/publish_to_adls.py --account sttelecaredevchn --container vendor-drop --source ./work/telecare/vendor-drop

# 2. build the bootstrap landing zone (as ADF would have written it, up to 15 Sep) - ~15 s, no Spark
python data_generator/build_landing.py --generated ./work/telecare --out ./work/bootstrap/landing --until 2026-09-15

# 3. ... and upload it -> landing
python data_generator/publish_to_adls.py --account sttelecaredevchn --container landing --source ./work/bootstrap/landing
```

Uploads take ~5–15 min depending on your connection (≈ 6,000 files each). They are re-runnable:
existing files are skipped.

✅ Storage browser shows `landing/telephony`, `landing/app`, `landing/devices`, `landing/crm`,
`landing/ehr/<8 tables>/load_date=…`.

---

## B7. Cluster policy (cost guardrail)

Databricks → **Compute → Policies → Create policy**
- Name: `telecare-single-node`
- Paste the JSON of `databricks/policies/telecare_single_node_policy.json`
- Create, then copy the **policy ID** from the URL or the policy page.

---

## B8. Configure and deploy the bundle

Edit `databricks/databricks.yml`, section `variables`:

```yaml
  cluster_policy_id:
    default: <paste the policy ID>
  alert_email:
    default: <your email>
```

Then:

```bash
cd databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

✅ Workflows → Jobs shows **`[dev <you>] telecare_ingestion_dev`** with 3 tasks:
`setup_catalog → bronze → silver`.

---

## B8b. PII group – do this BEFORE the first run

The date of birth is protected by a Unity Catalog column mask: only members of the group
`pii_readers` see it. **The identities that run the pipelines must be members** (they need the
real value to compute ages); analysts are not. Today the job and dbt run as *you*, so:

Databricks → your name (top right) → **Settings → Identity and access → Groups → Manage →
Add group** → name `pii_readers` → add **yourself** as member.
(If the button is not available, create it in the account console
`https://accounts.azuredatabricks.net` → User management → Groups.)

> If you forget this, nothing is silently lost: the rule `dob_not_null` quarantines every
> patient row and the circuit breaker stops the silver task with a clear error.

---

## B9. Run the job (≈ 20–30 min, the cluster stops itself)

```bash
databricks bundle run -t dev telecare_ingestion
```

Or: Jobs → the job → **Run now**. Watch the run graph; each task's log shows one line per feed.

What each task does:

| Task | Result |
|---|---|
| `setup_catalog` | schemas `bronze`, `silver`, `ops`; 16 silver tables; DQ tables; PII mask function |
| `bronze` | Auto Loader reads `landing/` → 16 bronze tables (+ masks `date_of_birth`) |
| `silver` | typing, unit/time-zone harmonisation, DQ rules, quarantine, de-duplication, MERGE |

---

## B10. Verify in the SQL editor (warehouse `wh-telecare-dev`)

```sql
-- 1. row counts per silver table
SELECT 'call_events' t, COUNT(*) n FROM telecare_dev.silver.call_events UNION ALL
SELECT 'app_events', COUNT(*) FROM telecare_dev.silver.app_events UNION ALL
SELECT 'device_measurements', COUNT(*) FROM telecare_dev.silver.device_measurements UNION ALL
SELECT 'ehr_encounter', COUNT(*) FROM telecare_dev.silver.ehr_encounter UNION ALL
SELECT 'ehr_encounter_diagnosis', COUNT(*) FROM telecare_dev.silver.ehr_encounter_diagnosis UNION ALL
SELECT 'crm_patient_coverage', COUNT(*) FROM telecare_dev.silver.crm_patient_coverage UNION ALL
SELECT 'crm_partners', COUNT(*) FROM telecare_dev.silver.crm_partners;

-- 2. which DQ rules fired
SELECT dataset, rule_name, severity, SUM(failed_rows) AS failed
FROM telecare_dev.ops.dq_rule_results
GROUP BY ALL HAVING SUM(failed_rows) > 0 ORDER BY dataset, failed DESC;

-- 3. what was quarantined, and why
SELECT dataset, dq_errors, LEFT(payload, 200) AS payload
FROM telecare_dev.ops.quarantine LIMIT 20;

-- 4. PII protection: mask + tag on the column
DESCRIBE TABLE EXTENDED telecare_dev.silver.ehr_patient date_of_birth;
SELECT patient_id, date_of_birth, canton FROM telecare_dev.silver.ehr_patient LIMIT 5;
-- demo for the screenshot: remove yourself from pii_readers, wait ~1 min, run the SELECT again
-- -> date_of_birth is NULL for you. Then add yourself back (dbt needs it).

-- 5. time-zone harmonisation: EHR local time is now UTC (2 h earlier in summer)
SELECT encounter_id, started_at FROM telecare_dev.silver.ehr_encounter ORDER BY started_at LIMIT 3;
```

Expected counts are listed in `docs/setup/02_expected_counts.md` (from the identical local run).
Small differences (< 0.1 %) are fine; paste your results in the chat.

Take screenshots of: the job run graph, query 2 and query 4. They go into the README later.

---

## Cost check at the end of the day
- Compute → **no running clusters** (job clusters vanish after the run)
- SQL Warehouses → `wh-telecare-dev` shows **Stopped** after 5 idle minutes
- Cost Management → the resource group's cost so far
