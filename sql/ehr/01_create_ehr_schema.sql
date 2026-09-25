/* =============================================================================
   TeleCare CH - simulated clinical system (EHR) on Azure SQL Database
   -----------------------------------------------------------------------------
   * Source-system style on purpose: timestamps are LOCAL Swiss time
     (Europe/Zurich, no offset), rows are soft-deleted (is_deleted), and every
     table carries created_at / modified_at, which ADF uses as the incremental
     watermark.
   * No foreign keys: the EHR modules are loaded independently (as in many real
     systems); referential integrity is tested downstream in dbt.
   * Idempotent: safe to run more than once.
   ========================================================================== */

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ehr') EXEC('CREATE SCHEMA ehr');
GO

IF OBJECT_ID('ehr.patient') IS NULL
CREATE TABLE ehr.patient (
    patient_id          varchar(12)   NOT NULL CONSTRAINT pk_patient PRIMARY KEY,
    date_of_birth       date          NULL,          -- PII: masked downstream
    sex                 char(1)       NULL,
    canton              char(2)       NULL,
    postal_code         varchar(10)   NULL,
    preferred_language  varchar(5)    NULL,
    is_deleted          bit           NOT NULL DEFAULT 0,
    created_at          datetime2(0)  NOT NULL,
    modified_at         datetime2(0)  NOT NULL
);
GO

IF OBJECT_ID('ehr.patient_identifier') IS NULL
CREATE TABLE ehr.patient_identifier (
    identifier_id       varchar(12)   NOT NULL CONSTRAINT pk_patient_identifier PRIMARY KEY,
    patient_id          varchar(12)   NOT NULL,
    identifier_type     varchar(20)   NOT NULL,      -- PHONE_HASH | APP_USER_ID
    identifier_value    varchar(128)  NOT NULL,
    verified_at         datetime2(0)  NULL,
    is_deleted          bit           NOT NULL DEFAULT 0,
    created_at          datetime2(0)  NOT NULL,
    modified_at         datetime2(0)  NOT NULL
);
GO

IF OBJECT_ID('ehr.staff') IS NULL
CREATE TABLE ehr.staff (
    staff_id            varchar(10)   NOT NULL CONSTRAINT pk_staff PRIMARY KEY,
    staff_login         varchar(20)   NOT NULL,
    role                varchar(20)   NOT NULL,      -- PHYSICIAN | MEDICAL_ASSISTANT
    specialty           varchar(40)   NULL,
    team                varchar(20)   NOT NULL,
    languages           varchar(20)   NULL,          -- comma separated
    employment_pct      tinyint       NULL,
    hired_at            datetime2(0)  NULL,
    left_at             datetime2(0)  NULL,
    is_deleted          bit           NOT NULL DEFAULT 0,
    created_at          datetime2(0)  NOT NULL,
    modified_at         datetime2(0)  NOT NULL
);
GO

IF OBJECT_ID('ehr.encounter') IS NULL
CREATE TABLE ehr.encounter (
    encounter_id        varchar(12)   NOT NULL CONSTRAINT pk_encounter PRIMARY KEY,
    patient_id          varchar(12)   NOT NULL,
    staff_id            varchar(10)   NULL,
    channel             varchar(10)   NOT NULL,      -- PHONE | VIDEO | CHAT
    service_line        varchar(20)   NOT NULL,
    contact_system      varchar(5)    NOT NULL,      -- TEL | APP
    contact_ref         varchar(40)   NULL,          -- call_id or app session_id
    started_at          datetime2(0)  NOT NULL,
    ended_at            datetime2(0)  NULL,
    triage_level        tinyint       NULL,
    disposition_code    varchar(20)   NULL,
    plan_code           varchar(12)   NULL,
    tariff_amount_chf   decimal(8,2)  NULL,
    status              varchar(10)   NOT NULL,      -- OPEN | CLOSED | CANCELLED
    is_deleted          bit           NOT NULL DEFAULT 0,
    created_at          datetime2(0)  NOT NULL,
    modified_at         datetime2(0)  NOT NULL
);
GO

IF OBJECT_ID('ehr.encounter_diagnosis') IS NULL
CREATE TABLE ehr.encounter_diagnosis (
    encounter_id        varchar(12)   NOT NULL,
    seq_no              tinyint       NOT NULL,
    icd10_code          varchar(10)   NOT NULL,
    diagnosis_role      varchar(10)   NOT NULL,      -- PRIMARY | SECONDARY
    is_deleted          bit           NOT NULL DEFAULT 0,
    created_at          datetime2(0)  NOT NULL,
    modified_at         datetime2(0)  NOT NULL,
    CONSTRAINT pk_encounter_diagnosis PRIMARY KEY (encounter_id, seq_no)
);
GO

IF OBJECT_ID('ehr.prescription') IS NULL
CREATE TABLE ehr.prescription (
    prescription_id     varchar(12)   NOT NULL CONSTRAINT pk_prescription PRIMARY KEY,
    encounter_id        varchar(12)   NOT NULL,
    atc_code            varchar(10)   NOT NULL,
    quantity            tinyint       NULL,
    pharmacy_partner_id varchar(10)   NULL,
    issued_at           datetime2(0)  NULL,
    is_deleted          bit           NOT NULL DEFAULT 0,
    created_at          datetime2(0)  NOT NULL,
    modified_at         datetime2(0)  NOT NULL
);
GO

IF OBJECT_ID('ehr.referral') IS NULL
CREATE TABLE ehr.referral (
    referral_id         varchar(12)   NOT NULL CONSTRAINT pk_referral PRIMARY KEY,
    encounter_id        varchar(12)   NOT NULL,
    partner_id          varchar(10)   NOT NULL,
    referral_type       varchar(20)   NOT NULL,
    urgency             varchar(12)   NULL,
    issued_at           datetime2(0)  NULL,
    is_deleted          bit           NOT NULL DEFAULT 0,
    created_at          datetime2(0)  NOT NULL,
    modified_at         datetime2(0)  NOT NULL
);
GO

IF OBJECT_ID('ehr.sick_note') IS NULL
CREATE TABLE ehr.sick_note (
    sick_note_id        varchar(12)   NOT NULL CONSTRAINT pk_sick_note PRIMARY KEY,
    encounter_id        varchar(12)   NOT NULL,
    incapacity_pct      tinyint       NULL,
    days                tinyint       NULL,
    valid_from          date          NULL,
    issued_at           datetime2(0)  NULL,
    is_deleted          bit           NOT NULL DEFAULT 0,
    created_at          datetime2(0)  NOT NULL,
    modified_at         datetime2(0)  NOT NULL
);
GO

/* Watermark indexes: ADF reads "WHERE modified_at > @last AND modified_at <= @now" */
DECLARE @t sysname, @sql nvarchar(400);
DECLARE c CURSOR LOCAL FAST_FORWARD FOR
    SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('ehr');
OPEN c; FETCH NEXT FROM c INTO @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_' + @t + '_modified_at')
    BEGIN
        SET @sql = N'CREATE INDEX ix_' + @t + N'_modified_at ON ehr.' + QUOTENAME(@t) + N' (modified_at)';
        EXEC sp_executesql @sql;
    END
    FETCH NEXT FROM c INTO @t;
END
CLOSE c; DEALLOCATE c;
GO
