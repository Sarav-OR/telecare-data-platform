"""Pure DataFrame transformations bronze -> silver, one function per feed.

No I/O in this module: every function takes and returns a DataFrame, so the logic
is unit-testable on a laptop and identical in streaming (foreachBatch), batch and
backfill runs.

Source quirks harmonised here
  * timestamps : ISO-8601 UTC (telephony, devices), epoch millis (app client),
                 local Swiss time without offset (EHR) -> all become UTC
  * dates      : ISO (devices), dd.MM.yyyy (CRM exports)
  * booleans   : "true"/"false", 0/1
  * units      : degF -> degC, glucose mmol/L -> mg/dL
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

SOURCE_TZ_EHR = "Europe/Zurich"
MMOL_TO_MGDL_GLUCOSE = 18.0182
STANDARD_UNITS = {"SBP": "mmHg", "DBP": "mmHg", "HR": "bpm", "SPO2": "%", "TEMP": "degC", "GLU": "mg/dL"}
_SNAPSHOT_RE = r"/(\d{4}-\d{2}-\d{2})/"


# ------------------------------------------------------------------------ helpers
def add_ingestion_metadata(df: DataFrame, run_id: str) -> DataFrame:
    """Lineage columns added when landing files enter bronze (`_metadata` = file-source metadata)."""
    return (df
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_orchestrator_run_id", F.lit(run_id)))


def s(c: str) -> Column:
    """Trimmed string, empty -> NULL."""
    v = F.trim(F.col(c).cast("string"))
    return F.when(v == "", None).otherwise(v)


def up(c: str) -> Column:
    return F.upper(s(c))


def boolean(c: str) -> Column:
    v = F.lower(s(c))
    return (F.when(v.isin("true", "1", "yes", "y"), True)
             .when(v.isin("false", "0", "no", "n"), False))


def iso_ts(c: str) -> Column:
    return F.to_timestamp(s(c))


def local_ts_to_utc(c: str) -> Column:
    """EHR stores wall-clock Swiss time; convert to UTC (DST-aware)."""
    return F.to_utc_timestamp(F.col(c).cast("timestamp"), SOURCE_TZ_EHR)


def swiss_date(c: str) -> Column:
    return F.to_date(s(c), "dd.MM.yyyy")


def snapshot_date() -> Column:
    return F.to_date(F.regexp_extract(F.col("_source_file"), _SNAPSHOT_RE, 1))


def _meta(df: DataFrame) -> list[Column]:
    cols = [F.col("_source_file").alias("source_file"), F.col("_ingested_at").alias("ingested_at")]
    if "_rescued_data" in df.columns:
        cols.insert(0, F.col("_rescued_data").alias("rescued_data"))
    else:
        cols.insert(0, F.lit(None).cast("string").alias("rescued_data"))
    return cols


def _corrupt(df: DataFrame) -> list[Column]:
    return [F.col("_corrupt_record")] if "_corrupt_record" in df.columns else []


# ------------------------------------------------------------------ S1 telephony
def std_call_events(df: DataFrame, **_) -> DataFrame:
    return df.select(
        s("event_id").alias("event_id"),
        up("call_id").alias("call_id"),
        s("sequence_no").cast("int").alias("sequence_no"),
        up("event_type").alias("event_type"),
        iso_ts("event_ts").alias("event_ts"),
        s("dialled_line").alias("dialled_line"),
        up("queue_code").alias("queue_code"),
        F.lower(s("agent_login")).alias("agent_login"),
        F.lower(s("caller_phone_hash")).alias("caller_phone_hash"),
        F.lower(s("ivr_language")).alias("ivr_language"),
        *_meta(df), *_corrupt(df))


# ------------------------------------------------------------------------ S2 app
def std_app_events(df: DataFrame, **_) -> DataFrame:
    props = F.col("properties")

    def prop(path: str) -> Column:
        return F.get_json_object(props, f"$.{path}")

    client_ts = F.timestamp_millis(s("client_ts").cast("long"))
    server_ts = iso_ts("server_ts")
    return df.select(
        s("event_id").alias("event_id"),
        s("session_id").alias("session_id"),
        s("app_user_id").alias("app_user_id"),
        F.lower(s("event_name")).alias("event_name"),
        client_ts.alias("client_ts"),
        server_ts.alias("server_ts"),
        F.lower(s("platform")).alias("platform"),
        s("app_version").alias("app_version"),
        F.upper(prop("service_line")).alias("service_line"),
        F.upper(prop("channel")).alias("channel"),
        F.upper(prop("symptom_category")).alias("symptom_category"),
        prop("triage_score").cast("int").alias("triage_score"),
        prop("wait_estimate_min").cast("int").alias("wait_estimate_min"),   # string in v4, int in v5
        prop("consent_version").alias("consent_version"),
        (F.coalesce(s("app_user_id").startswith("u_qa_"), F.lit(False))
         | F.coalesce(F.lower(prop("test_account")) == "true", F.lit(False))).alias("is_test_account"),
        (F.unix_timestamp(client_ts) - F.unix_timestamp(server_ts)).alias("client_clock_offset_s"),
        F.col("properties").alias("properties_json"),
        *_meta(df), *_corrupt(df))


# ---------------------------------------------------------------------- S5 devices
def std_device_measurements(df: DataFrame, late_threshold_minutes: int = 360, **_) -> DataFrame:
    raw = s("value").cast("double")
    unit = s("unit")
    metric = up("metric")
    value = (F.when(unit == "degF", (raw - 32) * 5 / 9)
              .when((metric == "GLU") & (unit == "mmol/L"), raw * MMOL_TO_MGDL_GLUCOSE)
              .otherwise(raw))
    std_unit = F.create_map(*[x for k, v in STANDARD_UNITS.items() for x in (F.lit(k), F.lit(v))])
    measured, received = iso_ts("measured_at"), iso_ts("received_at")
    return df.select(
        s("measurement_id").alias("measurement_id"),
        up("device_serial").alias("device_serial"),
        up("partner_id").alias("partner_id"),
        up("encounter_ref").alias("encounter_ref"),
        metric.alias("metric_code"),
        F.round(value, 2).alias("metric_value"),
        F.coalesce(std_unit[metric], unit).alias("unit"),
        raw.alias("original_value"),
        unit.alias("original_unit"),
        measured.alias("measured_at"),
        received.alias("received_at"),
        s("firmware_version").alias("firmware_version"),
        ((F.unix_timestamp(received) - F.unix_timestamp(measured)) > late_threshold_minutes * 60)
        .alias("is_late_arriving"),
        *_meta(df), *_corrupt(df))


def std_device_registry(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("device_serial").alias("device_serial"), s("model").alias("model"),
        s("manufacturer").alias("manufacturer"), s("firmware_version").alias("firmware_version"),
        up("partner_id").alias("partner_id"), up("status").alias("status"),
        F.to_date(s("installed_on")).alias("installed_on"), snapshot_date().alias("snapshot_date"),
        *_meta(df))


# -------------------------------------------------------------------------- S4 CRM
def std_crm_insurers(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("insurer_id").alias("insurer_id"), s("insurer_name").alias("insurer_name"),
        boolean("is_active").alias("is_active"), swiss_date("valid_since").alias("valid_since"),
        snapshot_date().alias("snapshot_date"), *_meta(df))


def std_crm_insurance_plans(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("plan_code").alias("plan_code"), up("insurer_id").alias("insurer_id"),
        s("plan_name").alias("plan_name"), up("model").alias("plan_model"),
        boolean("telmed_first_contact_required").alias("telmed_first_contact_required"),
        s("premium_discount_pct").cast("int").alias("premium_discount_pct"),
        snapshot_date().alias("snapshot_date"), *_meta(df))


def std_crm_patient_coverage(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("coverage_id").alias("coverage_id"), up("patient_id").alias("patient_id"),
        up("plan_code").alias("plan_code"), swiss_date("valid_from").alias("valid_from"),
        swiss_date("valid_to").alias("valid_to"), snapshot_date().alias("snapshot_date"), *_meta(df))


def std_crm_partners(df: DataFrame, **_) -> DataFrame:
    addr = F.col("address")
    return df.select(
        up("partnerId").alias("partner_id"), up("type").alias("partner_type"), s("name").alias("partner_name"),
        F.get_json_object(addr, "$.city").alias("city"),
        F.get_json_object(addr, "$.postalCode").alias("postal_code"),
        F.upper(F.get_json_object(addr, "$.canton")).alias("canton"),
        boolean("connectEnabled").alias("connect_enabled"), boolean("active").alias("is_active"),
        F.to_date(s("openedOn")).alias("opened_on"), F.to_date(s("deactivatedOn")).alias("deactivated_on"),
        F.coalesce(F.to_date(s("snapshot_date")), snapshot_date()).alias("snapshot_date"),
        *_meta(df), *_corrupt(df))


# -------------------------------------------------------------------------- S3 EHR
def _ehr_audit() -> list[Column]:
    return [F.coalesce(boolean("is_deleted"), F.lit(False)).alias("is_deleted"),
            local_ts_to_utc("created_at").alias("created_at"),
            local_ts_to_utc("modified_at").alias("modified_at")]


def std_ehr_patient(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("patient_id").alias("patient_id"),
        F.col("date_of_birth").cast("date").alias("date_of_birth"),        # PII - masked in Unity Catalog
        up("sex").alias("sex"), up("canton").alias("canton"), s("postal_code").alias("postal_code"),
        F.lower(s("preferred_language")).alias("preferred_language"), *_ehr_audit(), *_meta(df))


def std_ehr_patient_identifier(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("identifier_id").alias("identifier_id"), up("patient_id").alias("patient_id"),
        up("identifier_type").alias("identifier_type"), s("identifier_value").alias("identifier_value"),
        local_ts_to_utc("verified_at").alias("verified_at"), *_ehr_audit(), *_meta(df))


def std_ehr_staff(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("staff_id").alias("staff_id"), F.lower(s("staff_login")).alias("staff_login"),
        up("role").alias("role"), up("specialty").alias("specialty"), up("team").alias("team"),
        F.lower(s("languages")).alias("languages"), F.col("employment_pct").cast("int").alias("employment_pct"),
        local_ts_to_utc("hired_at").alias("hired_at"), local_ts_to_utc("left_at").alias("left_at"),
        *_ehr_audit(), *_meta(df))


def std_ehr_encounter(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("encounter_id").alias("encounter_id"), up("patient_id").alias("patient_id"),
        up("staff_id").alias("staff_id"), up("channel").alias("channel"), up("service_line").alias("service_line"),
        up("contact_system").alias("contact_system"), s("contact_ref").alias("contact_ref"),
        local_ts_to_utc("started_at").alias("started_at"), local_ts_to_utc("ended_at").alias("ended_at"),
        F.col("triage_level").cast("int").alias("triage_level"), up("disposition_code").alias("disposition_code"),
        up("plan_code").alias("plan_code"), F.col("tariff_amount_chf").cast("decimal(8,2)").alias("tariff_amount_chf"),
        up("status").alias("status"), *_ehr_audit(), *_meta(df))


def std_ehr_encounter_diagnosis(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("encounter_id").alias("encounter_id"), F.col("seq_no").cast("int").alias("seq_no"),
        up("icd10_code").alias("icd10_code"), up("diagnosis_role").alias("diagnosis_role"),
        *_ehr_audit(), *_meta(df))


def std_ehr_prescription(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("prescription_id").alias("prescription_id"), up("encounter_id").alias("encounter_id"),
        up("atc_code").alias("atc_code"), F.col("quantity").cast("int").alias("quantity"),
        up("pharmacy_partner_id").alias("pharmacy_partner_id"), local_ts_to_utc("issued_at").alias("issued_at"),
        *_ehr_audit(), *_meta(df))


def std_ehr_referral(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("referral_id").alias("referral_id"), up("encounter_id").alias("encounter_id"),
        up("partner_id").alias("partner_id"), up("referral_type").alias("referral_type"),
        up("urgency").alias("urgency"), local_ts_to_utc("issued_at").alias("issued_at"),
        *_ehr_audit(), *_meta(df))


def std_ehr_sick_note(df: DataFrame, **_) -> DataFrame:
    return df.select(
        up("sick_note_id").alias("sick_note_id"), up("encounter_id").alias("encounter_id"),
        F.col("incapacity_pct").cast("int").alias("incapacity_pct"), F.col("days").cast("int").alias("days"),
        F.col("valid_from").cast("date").alias("valid_from"), local_ts_to_utc("issued_at").alias("issued_at"),
        *_ehr_audit(), *_meta(df))


# ------------------------------------------------------------------------ dedupe
def deduplicate(df: DataFrame, keys: list[str], order_by: str) -> DataFrame:
    """Keep the first-received version of each key inside a batch.

    Cross-batch duplicates are handled by the insert-only MERGE on the same keys,
    so a re-sent record is ignored no matter how late it arrives.
    """
    w = Window.partitionBy(*keys).orderBy(F.col(order_by).asc_nulls_last())
    return df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")
