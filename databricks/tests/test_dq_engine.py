import pytest

from telecare_ingest.datasets import DATASETS
from telecare_ingest.dq import DataQualityThresholdError, DQEngine, Rule


def _rows(result_df, col="id"):
    return sorted(r[col] for r in result_df.collect())


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [(1, "DEV-000001", "HR", 72.0, 80),
         (2, None, "HR", 70.0, 5),
         (3, "BAD", "HR", 999.0, 90),
         (4, "DEV-000004", "XYZ", 36.6, None),
         (5, "DEV-000005", "SPO2", 101.0, 50)],
        "id int, device_serial string, metric_code string, metric_value double, battery_pct int")


def test_each_rule_type_and_null_semantics(spark, df):
    rules = [
        Rule("serial_nn", "not_null", column="device_serial"),
        Rule("serial_fmt", "regex", column="device_serial", pattern=r"^DEV-[0-9]{6}$"),
        Rule("metric_ok", "allowed_values", column="metric_code", values=("HR", "SPO2")),
        Rule("hr_range", "range", column="metric_value", min=20, max=250, where="metric_code = 'HR'"),
        Rule("spo2_range", "range", column="metric_value", max=100, where="metric_code = 'SPO2'"),
        Rule("battery_low", "range", column="battery_pct", min=10, severity="warn"),
    ]
    res = DQEngine("t", rules, ["id"]).run(df)
    errors = {r["id"]: set(r["_dq_errors"]) for r in res.evaluated.collect()}

    assert errors[1] == set()
    assert errors[2] == {"serial_nn"}                  # NULL reported once, not also by regex
    assert errors[3] == {"serial_fmt", "hr_range"}
    assert errors[4] == {"metric_ok"}                  # hr_range not applied: where-clause false
    assert errors[5] == {"spo2_range"}
    assert _rows(res.valid) == [1]
    assert res.total_rows == 5 and res.rejected_rows == 4


def test_warnings_do_not_reject_but_are_attached(spark, df):
    res = DQEngine("t", [Rule("battery_low", "range", column="battery_pct", min=10, severity="warn")],
                   ["id"]).run(df)
    assert res.rejected_rows == 0
    flagged = {r["id"]: r["dq_warnings"] for r in res.valid.collect()}
    assert flagged[2] == "battery_low"
    assert flagged[1] is None and flagged[4] is None   # NULL battery is not a warning


def test_expression_rule_treats_null_as_not_applicable(spark):
    df = spark.createDataFrame([(1, None), (2, 5), (3, -1)], "id int, x int")
    res = DQEngine("t", [Rule("x_pos", "expression", expression="x >= 0")], ["id"]).run(df)
    assert _rows(res.valid) == [1, 2]


def test_rule_metrics_count_per_rule(spark, df):
    engine = DQEngine("t", [Rule("serial_nn", "not_null", column="device_serial"),
                            Rule("metric_ok", "allowed_values", column="metric_code", values=("HR", "SPO2"))],
                      ["id"])
    res = engine.run(df)
    m = {x["rule_name"]: x for x in res.rule_metrics}
    assert m["serial_nn"]["failed_rows"] == 1
    assert m["metric_ok"]["failed_rows"] == 1
    assert m["metric_ok"]["failure_rate"] == pytest.approx(0.2)


def test_circuit_breaker(spark, df):
    engine = DQEngine("t", [Rule("serial_nn", "not_null", column="device_serial")], ["id"], max_error_rate=0.1)
    res = engine.run(df)                               # 1 / 5 = 20 % > 10 %
    with pytest.raises(DataQualityThresholdError, match="serial_nn=1"):
        engine.enforce_threshold(res)
    DQEngine("t", engine.rules, ["id"], max_error_rate=0.25).enforce_threshold(res)   # no raise


def test_quarantine_record_shape(spark, df):
    engine = DQEngine("t", [Rule("serial_nn", "not_null", column="device_serial")], ["id"])
    res = engine.run(df)
    q = engine.to_quarantine_records(res.quarantined, "run-1", 7).collect()
    assert len(q) == 1
    assert q[0]["record_key"] == '{"id":2}'
    assert q[0]["dq_errors"] == ["serial_nn"]
    assert q[0]["batch_id"] == 7 and q[0]["orchestrator_run_id"] == "run-1"


@pytest.mark.parametrize("bad", [
    {"name": "x", "type": "unknown"},
    {"name": "x", "type": "regex", "column": "a"},
    {"name": "x", "type": "range", "column": "a"},
    {"name": "x", "type": "not_null", "column": "a", "severity": "fatal"},
])
def test_invalid_rule_definitions_fail_fast(bad):
    with pytest.raises(ValueError):
        Rule.from_dict(bad)


def test_duplicate_rule_names_rejected():
    r = Rule("a", "not_null", column="x")
    with pytest.raises(ValueError, match="duplicate"):
        DQEngine("t", [r, r], ["x"])


@pytest.mark.parametrize("dataset", sorted(DATASETS))
def test_packaged_rules_load_for_every_registered_dataset(dataset):
    engine = DQEngine.from_yaml(dataset)
    assert engine.rules and engine.key_columns
    assert DQEngine.from_yaml(dataset, max_error_rate_override=0.5).max_error_rate == 0.5
