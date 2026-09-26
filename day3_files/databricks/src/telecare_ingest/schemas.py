"""Landing schemas per feed.

File feeds (JSON / CSV) are read as STRING on purpose: a type change at a vendor
must never make ingestion drop records. Typing happens in silver, where failures
are visible (DQ rules) instead of silent. Unknown fields are captured by Auto
Loader in `_rescued_data`, malformed JSON lines in `_corrupt_record`.

EHR feeds arrive as Parquet written by ADF from Azure SQL, so they carry the
source types; their schemas below document what ADF produces (used for table
creation and tests - Auto Loader reads the Parquet schema itself).
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _strings(*names: str) -> StructType:
    return StructType([StructField(n, StringType(), True) for n in names])


def _typed(*fields: tuple) -> StructType:
    return StructType([StructField(n, t, True) for n, t in fields])


S, TS, D, INT, B = StringType(), TimestampType(), DateType(), IntegerType(), BooleanType()
_AUDIT = (("is_deleted", B), ("created_at", TS), ("modified_at", TS))

LANDING = {
    # S1 telephony
    "call_events": _strings("event_id", "call_id", "sequence_no", "event_type", "event_ts", "dialled_line",
                            "queue_code", "agent_login", "caller_phone_hash", "ivr_language", "_corrupt_record"),
    # S2 app (nested `properties` is read as raw JSON text and parsed in silver)
    "app_events": _strings("event_id", "session_id", "app_user_id", "event_name", "client_ts", "server_ts",
                           "platform", "app_version", "properties", "_corrupt_record"),
    # S5 devices
    "device_measurements": _strings("measurement_id", "device_serial", "partner_id", "encounter_ref", "metric",
                                    "value", "unit", "measured_at", "received_at", "firmware_version",
                                    "_corrupt_record"),
    "device_registry": _strings("device_serial", "model", "manufacturer", "firmware_version", "partner_id",
                                "status", "installed_on"),
    # S4 CRM (CSV ';' with dd.mm.yyyy dates) + partner REST API (JSON, one object per line)
    "crm_insurers": _strings("insurer_id", "insurer_name", "is_active", "valid_since"),
    "crm_insurance_plans": _strings("plan_code", "insurer_id", "plan_name", "model",
                                    "telmed_first_contact_required", "premium_discount_pct"),
    "crm_patient_coverage": _strings("coverage_id", "patient_id", "plan_code", "valid_from", "valid_to"),
    # nested `address` (bootstrap files) or flat city/postalCode/canton (ADF REST copy): additive contract
    "crm_partners": _strings("partnerId", "type", "name", "address", "city", "postalCode", "canton",
                             "connectEnabled", "active", "openedOn", "deactivatedOn", "snapshot_date",
                             "_corrupt_record"),
    # S3 EHR (Parquet from ADF, typed; timestamps are local Swiss time without offset)
    "ehr_patient": _typed(("patient_id", S), ("date_of_birth", D), ("sex", S), ("canton", S), ("postal_code", S),
                          ("preferred_language", S), *_AUDIT),
    "ehr_patient_identifier": _typed(("identifier_id", S), ("patient_id", S), ("identifier_type", S),
                                     ("identifier_value", S), ("verified_at", TS), *_AUDIT),
    "ehr_staff": _typed(("staff_id", S), ("staff_login", S), ("role", S), ("specialty", S), ("team", S),
                        ("languages", S), ("employment_pct", INT), ("hired_at", TS), ("left_at", TS), *_AUDIT),
    "ehr_encounter": _typed(("encounter_id", S), ("patient_id", S), ("staff_id", S), ("channel", S),
                            ("service_line", S), ("contact_system", S), ("contact_ref", S), ("started_at", TS),
                            ("ended_at", TS), ("triage_level", INT), ("disposition_code", S), ("plan_code", S),
                            ("tariff_amount_chf", DecimalType(8, 2)), ("status", S), *_AUDIT),
    "ehr_encounter_diagnosis": _typed(("encounter_id", S), ("seq_no", INT), ("icd10_code", S),
                                      ("diagnosis_role", S), *_AUDIT),
    "ehr_prescription": _typed(("prescription_id", S), ("encounter_id", S), ("atc_code", S), ("quantity", INT),
                               ("pharmacy_partner_id", S), ("issued_at", TS), *_AUDIT),
    "ehr_referral": _typed(("referral_id", S), ("encounter_id", S), ("partner_id", S), ("referral_type", S),
                           ("urgency", S), ("issued_at", TS), *_AUDIT),
    "ehr_sick_note": _typed(("sick_note_id", S), ("encounter_id", S), ("incapacity_pct", INT), ("days", INT),
                            ("valid_from", D), ("issued_at", TS), *_AUDIT),
}
