/* =============================================================================
   TeleCare CH - orchestration control tables for Azure Data Factory
   -----------------------------------------------------------------------------
   Metadata-driven orchestration: ADF reads WHAT to load from these tables and
   writes back HOW FAR it got. Adding a feed = inserting a row, not editing a
   pipeline.

     ctl.batch_state   processing date of the daily batch (the "business clock")
     ctl.file_feed     file feeds copied vendor-drop -> landing, one row per feed; a feed that
                       is due on the run date but missing fails the pipeline (late-delivery alert)
     ctl.sql_extract   EHR tables extracted incrementally, with their watermark
     ctl.run_log       one row per pipeline step: audit trail and ops dashboard

   Lives in the same database as the simulated EHR only to save cost; in a real
   platform it sits in a separate small "control" database owned by the data team.
   Idempotent: safe to run more than once (seed rows are only inserted if missing).
   ========================================================================== */

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ctl') EXEC('CREATE SCHEMA ctl');
GO

/* ---------------------------------------------------------------- batch state */
IF OBJECT_ID('ctl.batch_state') IS NULL
CREATE TABLE ctl.batch_state (
    batch_name           varchar(50)  NOT NULL CONSTRAINT pk_batch_state PRIMARY KEY,
    last_completed_date  date         NOT NULL,   -- last business date fully loaded
    end_date             date         NOT NULL,   -- last date the sources have data for (simulation horizon)
    last_run_id          varchar(64)  NULL,
    updated_at           datetime2(0) NOT NULL CONSTRAINT df_batch_state_upd DEFAULT sysutcdatetime()
);
GO
IF NOT EXISTS (SELECT 1 FROM ctl.batch_state WHERE batch_name = 'telecare_daily')
    INSERT ctl.batch_state (batch_name, last_completed_date, end_date)
    VALUES ('telecare_daily', '2026-09-15', '2026-09-22');   -- Day 2 bootstrap covered up to 15 Sep
GO

/* ---------------------------------------------------------------- file feeds */
IF OBJECT_ID('ctl.file_feed') IS NULL
CREATE TABLE ctl.file_feed (
    feed_name         varchar(50)  NOT NULL CONSTRAINT pk_file_feed PRIMARY KEY,
    source_system     varchar(20)  NOT NULL,
    folder_prefix     varchar(200) NOT NULL,   -- path inside vendor-drop and landing
    date_path_format  varchar(20)  NOT NULL,   -- .NET format of the date part of the path
    delivery_day      varchar(10)  NULL,       -- NULL = delivered daily; 'Monday' = weekly snapshot
    is_active         bit          NOT NULL CONSTRAINT df_file_feed_active DEFAULT 1
);
GO
MERGE ctl.file_feed AS t
USING (VALUES
    ('call_events',          'telephony', 'telephony/call_events',    'yyyy/MM/dd', NULL),
    ('app_events',           'app',       'app/app_events',           'yyyy/MM/dd', NULL),
    ('device_measurements',  'devices',   'devices/measurements',     'yyyy/MM/dd', NULL),
    ('device_registry',      'devices',   'devices/device_registry',  'yyyy-MM-dd', 'Monday'),
    ('crm_insurers',         'crm',       'crm/insurers',             'yyyy-MM-dd', 'Monday'),
    ('crm_insurance_plans',  'crm',       'crm/insurance_plans',      'yyyy-MM-dd', 'Monday'),
    ('crm_patient_coverage', 'crm',       'crm/patient_coverage',     'yyyy-MM-dd', 'Monday')
) AS s (feed_name, source_system, folder_prefix, date_path_format, delivery_day)
ON t.feed_name = s.feed_name
WHEN NOT MATCHED THEN INSERT (feed_name, source_system, folder_prefix, date_path_format, delivery_day)
     VALUES (s.feed_name, s.source_system, s.folder_prefix, s.date_path_format, s.delivery_day);
GO

/* ---------------------------------------------------------------- SQL extracts */
IF OBJECT_ID('ctl.sql_extract') IS NULL
CREATE TABLE ctl.sql_extract (
    table_name        sysname       NOT NULL CONSTRAINT pk_sql_extract PRIMARY KEY,
    select_list       nvarchar(max) NOT NULL,  -- explicit columns: a new source column never breaks the load
    watermark_column  sysname       NOT NULL CONSTRAINT df_sql_extract_wmcol DEFAULT 'modified_at',
    watermark_value   datetime2(0)  NOT NULL,  -- exclusive upper bound of the last successful extract
    last_row_count    int           NULL,
    is_active         bit           NOT NULL CONSTRAINT df_sql_extract_active DEFAULT 1,
    updated_at        datetime2(0)  NOT NULL CONSTRAINT df_sql_extract_upd DEFAULT sysutcdatetime()
);
GO
/* tinyint is cast to int: the landing contract (and the Day 2 bootstrap files) use 32-bit integers */
MERGE ctl.sql_extract AS t
USING (VALUES
    ('patient',             N'patient_id, date_of_birth, sex, canton, postal_code, preferred_language, is_deleted, created_at, modified_at'),
    ('patient_identifier',  N'identifier_id, patient_id, identifier_type, identifier_value, verified_at, is_deleted, created_at, modified_at'),
    ('staff',               N'staff_id, staff_login, role, specialty, team, languages, CAST(employment_pct AS int) AS employment_pct, hired_at, left_at, is_deleted, created_at, modified_at'),
    ('encounter',           N'encounter_id, patient_id, staff_id, channel, service_line, contact_system, contact_ref, started_at, ended_at, CAST(triage_level AS int) AS triage_level, disposition_code, plan_code, tariff_amount_chf, status, is_deleted, created_at, modified_at'),
    ('encounter_diagnosis', N'encounter_id, CAST(seq_no AS int) AS seq_no, icd10_code, diagnosis_role, is_deleted, created_at, modified_at'),
    ('prescription',        N'prescription_id, encounter_id, atc_code, CAST(quantity AS int) AS quantity, pharmacy_partner_id, issued_at, is_deleted, created_at, modified_at'),
    ('referral',            N'referral_id, encounter_id, partner_id, referral_type, urgency, issued_at, is_deleted, created_at, modified_at'),
    ('sick_note',           N'sick_note_id, encounter_id, CAST(incapacity_pct AS int) AS incapacity_pct, CAST(days AS int) AS days, valid_from, issued_at, is_deleted, created_at, modified_at')
) AS s (table_name, select_list)
ON t.table_name = s.table_name
WHEN NOT MATCHED THEN INSERT (table_name, select_list, watermark_value)
     VALUES (s.table_name, s.select_list, '2026-09-16 00:00:00');   -- bootstrap loaded everything before 16 Sep
GO

/* ---------------------------------------------------------------- run log */
IF OBJECT_ID('ctl.run_log') IS NULL
CREATE TABLE ctl.run_log (
    log_id        bigint IDENTITY(1,1) NOT NULL CONSTRAINT pk_run_log PRIMARY KEY,
    run_id        varchar(64)    NOT NULL,     -- ADF pipeline run id (also passed to Databricks)
    pipeline_name varchar(100)   NOT NULL,
    run_date      date           NULL,
    step          varchar(100)   NOT NULL,
    status        varchar(20)    NOT NULL,     -- STARTED | SUCCEEDED | FAILED | SKIPPED
    rows_copied   bigint         NULL,
    message       nvarchar(4000) NULL,
    logged_at     datetime2(0)   NOT NULL CONSTRAINT df_run_log_at DEFAULT sysutcdatetime()
);
GO

/* ---------------------------------------------------------------- procedures */
CREATE OR ALTER PROCEDURE ctl.usp_log_step
    @run_id varchar(64), @pipeline_name varchar(100), @run_date date, @step varchar(100),
    @status varchar(20), @rows_copied bigint = NULL, @message nvarchar(4000) = NULL
AS
    INSERT ctl.run_log (run_id, pipeline_name, run_date, step, status, rows_copied, message)
    VALUES (@run_id, @pipeline_name, @run_date, @step, @status, @rows_copied, LEFT(@message, 4000));
GO

CREATE OR ALTER PROCEDURE ctl.usp_set_watermark
    @table_name sysname, @watermark_value datetime2(0), @row_count int
AS
    UPDATE ctl.sql_extract
    SET watermark_value = @watermark_value, last_row_count = @row_count, updated_at = sysutcdatetime()
    WHERE table_name = @table_name;
GO

/* Moves the business clock forward only if the date is the next one: a re-run of an old
   date, or two overlapping runs, can never skip or repeat a day. */
CREATE OR ALTER PROCEDURE ctl.usp_complete_batch
    @batch_name varchar(50), @run_date date, @run_id varchar(64)
AS
BEGIN
    UPDATE ctl.batch_state
    SET last_completed_date = @run_date, last_run_id = @run_id, updated_at = sysutcdatetime()
    WHERE batch_name = @batch_name AND last_completed_date = DATEADD(day, -1, @run_date);
    IF @@ROWCOUNT = 0
        THROW 50001, 'Batch state was not advanced: run_date is not the next business date.', 1;
END
GO
