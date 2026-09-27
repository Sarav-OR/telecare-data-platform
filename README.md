# TeleCare CH – Data Platform (ADF · Databricks · dbt Cloud)

[![ci](https://github.com/Sarav-OR/telecare-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Sarav-OR/telecare-data-platform/actions/workflows/ci.yml)

Production-style data platform for a **fictional Swiss telemedicine provider**, built to
demonstrate a Data Vault → Kimball migration on Azure. All data is synthetic.

> Status: **Day 2 of 5** – generator, clinical database, Databricks ingestion (16 feeds) and the
> complete dbt project (16 staging views, 37 raw-vault tables, 3 business-vault tables, 22 Kimball
> tables; 168 data tests + 1 unit test + 5 vault-vs-Kimball reconciliation tests) are built and
> tested locally. ADF (Day 3), CI/CD (Day 4) and documentation/Power BI (Day 5) follow.

## What's in the repo today

| Folder | Content |
|---|---|
| `docs/` | Setup guide (Azure), data model specification |
| `data_generator/` | 5-source synthetic data generator + loader for the Azure SQL clinical database |
| `sql/ehr/` | Azure SQL schema of the simulated clinical system (EHR) + ADF read grant |
| `databricks/` | Ingestion package (bronze/silver, DQ engine), Asset Bundle, cluster policy, tests |
| `dbt/` | Staging → raw vault → business vault → Kimball marts, seeds, macros, tests |
| `config/environments/` | All environment-specific values (dev, prd) in one place |

## Run it locally (no cloud cost)

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

# 1. generate 90 days of data for 5 source systems (~45 s, ~470 MB)
python data_generator/generate.py --out ./work/telecare

# 2. unit tests for the ingestion framework (57 tests)
cd databricks && python -m pytest -q && cd ..

# 3. simulate ADF landing, then bronze -> silver for all 16 feeds with DQ (~4 min)
python databricks/scripts/run_local_pipeline.py --generated ./work/telecare --out ./work/lakehouse

# 4. load silver into DuckDB and build + test the whole dbt project (~1 min)
python dbt/scripts/load_silver_to_duckdb.py --lakehouse ./work/lakehouse --db ./work/telecare_ci.duckdb
cd dbt && DBT_TARGET=ci DBT_DUCKDB_PATH=../work/telecare_ci.duckdb DBT_PROFILES_DIR=. dbt build && cd ..
```

Proof that incremental loads equal a full rebuild (late-arriving data):

```bash
python databricks/scripts/run_local_pipeline.py --generated ./work/telecare --out ./work/bootstrap_run --until 2026-09-15
python dbt/scripts/check_incremental_equivalence.py --initial ./work/bootstrap_run --final ./work/lakehouse --workdir ./work/inc_check
```
