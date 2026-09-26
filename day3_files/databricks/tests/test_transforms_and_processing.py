import json
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from telecare_ingest.config import JobConfig
from telecare_ingest.datasets import DATASETS
from telecare_ingest.dq import DataQualityThresholdError
from telecare_ingest.jobs.setup_catalog import silver_ddl, silver_schema
from telecare_ingest.processing import process_batch
from telecare_ingest.transforms import (
    deduplicate,
    std_app_events,
    std_call_events,
    std_crm_patient_coverage,
    std_device_measurements,
    std_ehr_encounter,
)

LAND = "abfss://landing@acc.dfs.core.windows.net/"


def _with_meta(df, source_file):
    return (df.withColumn("_source_file", F.lit(source_file))
              .withColumn("_ingested_at", F.to_timestamp(F.lit("2026-09-30 00:00:00")))
              .withColumn("_rescued_data", F.lit(None).cast("string")))


@pytest.fixture
def cfg(tmp_path):
    return JobConfig(env="dev", catalog="telecare_dev", landing_root=str(tmp_path),
                     checkpoint_root=str(tmp_path / "chk"), orchestrator_run_id="test-run")


# ----------------------------------------------------------------------------- telephony
def _call(event_id="e1", call_id="call-1", seq="1", etype="offered", ts="2026-09-01T10:00:00.000Z",
          queue="Q_GEN_DE", phone="a" * 64, corrupt=None):
    return (event_id, call_id, seq, etype, ts, "+41610000100", queue, "TC0001", phone, "DE", corrupt)


def _calls(spark, rows):
    cols = ("event_id string, call_id string, sequence_no string, event_type string, event_ts string, "
            "dialled_line string, queue_code string, agent_login string, caller_phone_hash string, "
            "ivr_language string, _corrupt_record string")
    return _with_meta(spark.createDataFrame(rows, cols), LAND + "telephony/call_events/2026/09/01/10/x.jsonl")


def test_call_events_are_typed_and_normalised(spark):
    r = std_call_events(_calls(spark, [_call()])).collect()[0]
    assert r["event_ts"] == datetime(2026, 9, 1, 10, 0, 0)
    assert r["call_id"] == "CALL-1" and r["event_type"] == "OFFERED"
    assert r["agent_login"] == "tc0001" and r["ivr_language"] == "de" and r["sequence_no"] == 1


# ----------------------------------------------------------------------------------- app
def _app(spark, rows):
    cols = ("event_id string, session_id string, app_user_id string, event_name string, client_ts string, "
            "server_ts string, platform string, app_version string, properties string, _corrupt_record string")
    return _with_meta(spark.createDataFrame(rows, cols), LAND + "app/app_events/2026/09/01/10/x.jsonl")


def test_app_events_parse_nested_properties_across_versions(spark):
    v4 = '{"service_line":"MED_CENTER","channel":"CHAT","wait_estimate_min":"12"}'
    v5 = '{"service_line":"KIDS_LINE","channel":"VIDEO","wait_estimate_min":7,"triage_score":3}'
    qa = '{"test_account":true}'
    epoch = str(int(datetime(2026, 9, 1, 10, 0, 0).timestamp() * 1000))   # session TZ is UTC in tests
    out = std_app_events(_app(spark, [
        ("a", "s1", "u_1", "APP_OPEN", epoch, "2026-09-01T10:00:01.000Z", "IOS", "4.8.1", v4, None),
        ("b", "s2", "u_2", "chat_started", epoch, "2026-09-01T13:00:00.000Z", "web", "5.0.1", v5, None),
        ("c", "s3", "u_qa_9", "app_open", epoch, "2026-09-01T10:00:00.000Z", "web", "5.0.1", qa, None),
    ]))
    r = {x["event_id"]: x for x in out.collect()}
    assert r["a"]["wait_estimate_min"] == 12 and r["b"]["wait_estimate_min"] == 7    # string and int
    assert r["a"]["triage_score"] is None and r["b"]["triage_score"] == 3             # new field in v5
    assert r["a"]["event_name"] == "app_open" and r["a"]["platform"] == "ios"
    assert r["b"]["client_clock_offset_s"] == -3 * 3600                               # skewed device clock
    assert r["c"]["is_test_account"] and not r["a"]["is_test_account"]


# ------------------------------------------------------------------------------- devices
def test_device_unit_harmonisation_and_late_flag(spark):
    cols = ("measurement_id string, device_serial string, partner_id string, encounter_ref string, metric string, "
            "value string, unit string, measured_at string, received_at string, firmware_version string, "
            "_corrupt_record string")
    rows = [("m1", "tcd-000001", "prt00001", "e1", "TEMP", "98.6", "degF", "2026-09-01T10:00:00Z",
             "2026-09-01T10:01:00.000Z", "1.9.3", None),
            ("m2", "TCD-000001", "PRT00001", "E1", "GLU", "7.0", "mmol/L", "2026-09-01T10:00:00Z",
             "2026-09-01T20:00:00.000Z", "1.9.3", None)]
    df = _with_meta(spark.createDataFrame(rows, cols), LAND + "devices/measurements/x.jsonl")
    r = {x["measurement_id"]: x for x in std_device_measurements(df, late_threshold_minutes=360).collect()}
    assert r["m1"]["metric_value"] == pytest.approx(37.0) and r["m1"]["unit"] == "degC"
    assert r["m1"]["device_serial"] == "TCD-000001" and r["m1"]["original_unit"] == "degF"
    assert r["m2"]["metric_value"] == pytest.approx(126.13) and r["m2"]["unit"] == "mg/dL"
    assert (r["m1"]["is_late_arriving"], r["m2"]["is_late_arriving"]) == (False, True)


# ----------------------------------------------------------------------------------- CRM
def test_crm_swiss_dates_and_snapshot_date_from_path(spark):
    df = spark.createDataFrame([("cov1", "p0000001", "ins01-tel", "01.08.2026", "")],
                               "coverage_id string, patient_id string, plan_code string, valid_from string, "
                               "valid_to string")
    df = _with_meta(df, LAND + "crm/patient_coverage/2026-08-03/patient_coverage.csv")
    r = std_crm_patient_coverage(df).collect()[0]
    assert str(r["valid_from"]) == "2026-08-01" and r["valid_to"] is None
    assert str(r["snapshot_date"]) == "2026-08-03" and r["plan_code"] == "INS01-TEL"


# ----------------------------------------------------------------------------------- EHR
def test_ehr_local_time_is_converted_to_utc_with_dst(spark):
    schema = DATASETS["ehr_encounter"].landing_schema
    base = {f.name: None for f in schema.fields}
    summer = dict(base, encounter_id="e1", patient_id="p1", channel="phone", service_line="MED_CENTER",
                  status="CLOSED", is_deleted=False, started_at=datetime(2026, 7, 1, 12, 0),
                  created_at=datetime(2026, 7, 1, 12, 0), modified_at=datetime(2026, 7, 1, 12, 30))
    winter = dict(summer, encounter_id="e2", started_at=datetime(2026, 1, 15, 12, 0))
    df = _with_meta(spark.createDataFrame([summer, winter], schema), LAND + "ehr/encounter/load_date=x/p.parquet")
    r = {x["encounter_id"]: x for x in std_ehr_encounter(df).collect()}
    assert r["E1"]["started_at"] == datetime(2026, 7, 1, 10, 0)      # CEST = UTC+2
    assert r["E2"]["started_at"] == datetime(2026, 1, 15, 11, 0)     # CET  = UTC+1
    assert r["E1"]["channel"] == "PHONE" and r["E1"]["is_deleted"] is False


# ------------------------------------------------------------------------ table contracts
@pytest.mark.parametrize("name", sorted(DATASETS))
def test_every_feed_has_a_silver_contract(spark, name):
    spec = DATASETS[name]
    cols = [f.name for f in silver_schema(spark, spec).fields]
    assert "dq_warnings" in cols and "source_file" in cols and "_corrupt_record" not in cols
    assert set(spec.merge_keys) <= set(cols), "merge keys must exist in silver"
    ddl = silver_ddl(spark, "telecare_dev", spec)
    assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS telecare_dev.silver.{name}")
    assert ("CLUSTER BY" in ddl) == bool(spec.cluster_by)


def test_dedupe_keeps_first_received(spark):
    df = spark.createDataFrame([("e1", 2), ("e1", 1), ("e2", 5)], "event_id string, ingested_at int")
    out = {r["event_id"]: r["ingested_at"] for r in deduplicate(df, ["event_id"], "ingested_at").collect()}
    assert out == {"e1": 1, "e2": 5}


# ------------------------------------------------------------------------ end-to-end batch
def test_process_batch_end_to_end(spark, cfg, sink):
    cfg = JobConfig(**{**cfg.__dict__, "max_error_rate": 0.10})     # 2 bad rows of 45 is intended here
    rows = [_call(f"ok{i}") for i in range(40)] + [
        _call("dup"), _call("dup"),                                   # re-delivered event
        _call("bad_phone", phone="not-a-hash"),                        # error -> quarantine
        _call(None, corrupt='{"event_id":"trunc'),                     # malformed line -> quarantine
        _call("oldq", queue="Q_LEGACY_99"),                            # warning only
    ]
    summary = process_batch(spark, _calls(spark, rows), 3, DATASETS["call_events"], cfg, sink)

    silver = {r["event_id"]: r for r in sink.merged["telecare_dev.silver.call_events"]}
    assert len(silver) == 42 and "bad_phone" not in silver
    assert silver["oldq"]["dq_warnings"] == "queue_code_known"
    quarantine = sink.appended["telecare_dev.ops.quarantine"]
    reasons = {r for q in quarantine for r in q["dq_errors"]}
    assert {"caller_hash_format", "record_parseable"} <= reasons
    assert json.loads(quarantine[0]["payload"])                         # replayable JSON
    assert summary["rows_in"] == 45 and summary["rows_rejected"] == 2


def test_circuit_breaker_blocks_silver_but_keeps_evidence(spark, cfg, sink):
    rows = [_call(f"bad{i}", phone="x") for i in range(5)] + [_call("good")]
    with pytest.raises(DataQualityThresholdError):
        process_batch(spark, _calls(spark, rows), 1, DATASETS["call_events"], cfg, sink)
    assert "telecare_dev.silver.call_events" not in sink.merged         # nothing half-loaded
    assert len(sink.appended["telecare_dev.ops.quarantine"]) == 5        # evidence kept


def test_recover_type_drift_restores_rescued_values(spark):
    """ADF writes SQL date as INT96 -> Auto Loader rescues it; the value must come back typed."""
    from datetime import date

    from telecare_ingest.transforms import recover_type_drift

    df = spark.createDataFrame(
        [("P0000001", None, None, '{"date_of_birth":"1985-03-02T00:00:00.000Z","triage_level":"3",'
                                  '"_file_path":"x"}'),
         ("P0000002", date(1990, 1, 1), 2, None)],
        "patient_id string, date_of_birth date, triage_level int, _rescued_data string")
    out = {r.patient_id: r for r in recover_type_drift(df).collect()}
    assert out["P0000001"].date_of_birth == date(1985, 3, 2)
    assert out["P0000001"].triage_level == 3
    assert out["P0000002"].date_of_birth == date(1990, 1, 1)
