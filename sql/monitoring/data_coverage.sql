-- =============================================================================
-- Data coverage: "up to which business date is every layer loaded?"
-- Run in the Databricks SQL editor (warehouse wh-telecare-dev), catalog telecare_dev.
-- Two different clocks per table:
--   business date  = when it happened (event / encounter / snapshot date)
--   loaded at      = when OUR pipeline wrote it (ingestion / dbt load time)
-- =============================================================================

-- 1. SILVER (Databricks ingestion): business date and last load per feed ------------------
SELECT 'call_events' AS feed, MAX(CAST(event_ts AS DATE)) AS max_business_date, MAX(ingested_at) AS last_loaded_at, COUNT(*) AS row_count FROM telecare_dev.silver.call_events
UNION ALL SELECT 'app_events',              MAX(CAST(server_ts AS DATE)),   MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.app_events
UNION ALL SELECT 'device_measurements',     MAX(CAST(measured_at AS DATE)), MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.device_measurements
UNION ALL SELECT 'device_registry',         MAX(snapshot_date),             MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.device_registry
UNION ALL SELECT 'crm_insurers',            MAX(snapshot_date),             MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.crm_insurers
UNION ALL SELECT 'crm_insurance_plans',     MAX(snapshot_date),             MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.crm_insurance_plans
UNION ALL SELECT 'crm_patient_coverage',    MAX(snapshot_date),             MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.crm_patient_coverage
UNION ALL SELECT 'crm_partners',            MAX(snapshot_date),             MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.crm_partners
UNION ALL SELECT 'ehr_encounter',           MAX(CAST(started_at AS DATE)),  MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.ehr_encounter
UNION ALL SELECT 'ehr_encounter_diagnosis', MAX(CAST(modified_at AS DATE)), MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.ehr_encounter_diagnosis
UNION ALL SELECT 'ehr_patient',             MAX(CAST(modified_at AS DATE)), MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.ehr_patient
UNION ALL SELECT 'ehr_prescription',        MAX(CAST(modified_at AS DATE)), MAX(ingested_at), COUNT(*) FROM telecare_dev.silver.ehr_prescription
ORDER BY feed;

-- 2. BRONZE: which landing files were read last (per feed, by source file path) --------------
SELECT 'call_events' AS feed, MAX(_source_file) AS last_file, MAX(_ingested_at) AS last_loaded_at FROM telecare_dev.bronze.call_events
UNION ALL SELECT 'ehr_encounter', MAX(_source_file), MAX(_ingested_at) FROM telecare_dev.bronze.ehr_encounter
UNION ALL SELECT 'crm_partners',  MAX(_source_file), MAX(_ingested_at) FROM telecare_dev.bronze.crm_partners;

-- 3. RAW VAULT (dbt): last load per object (technical load time) -----------------------------
SELECT 'hub_encounter' AS object, MAX(load_datetime) AS last_load, COUNT(*) AS row_count FROM telecare_dev.raw_vault.hub_encounter
UNION ALL SELECT 'hub_patient',        MAX(load_datetime), COUNT(*) FROM telecare_dev.raw_vault.hub_patient
UNION ALL SELECT 'sat_encounter',      MAX(load_datetime), COUNT(*) FROM telecare_dev.raw_vault.sat_encounter
UNION ALL SELECT 'nhl_call_event',     MAX(load_datetime), COUNT(*) FROM telecare_dev.raw_vault.nhl_call_event
ORDER BY object;

-- 4. MARTS (dbt, what Power BI sees): business date coverage per fact -------------------------
SELECT 'fact_encounter' AS fact, MIN(date_key) AS first_day, MAX(date_key) AS last_day, COUNT(*) AS row_count, MAX(_loaded_at) AS last_loaded_at FROM telecare_dev.marts.fact_encounter
UNION ALL SELECT 'fact_contact',                MIN(date_key), MAX(date_key), COUNT(*), MAX(_loaded_at) FROM telecare_dev.marts.fact_contact
UNION ALL SELECT 'fact_diagnostic_measurement', MIN(date_key), MAX(date_key), COUNT(*), MAX(_loaded_at) FROM telecare_dev.marts.fact_diagnostic_measurement;

-- 5. Rows per business day in the last week (every day should be there, similar size) ----------
SELECT f.date_key, COUNT(*) AS encounters
FROM telecare_dev.marts.fact_encounter f
WHERE f.date_key >= 20260914
GROUP BY f.date_key ORDER BY f.date_key;
