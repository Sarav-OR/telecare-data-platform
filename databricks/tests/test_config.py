import pytest

from telecare_ingest.config import ConfigError, JobConfig

BASE = ["--env", "dev", "--catalog", "telecare_dev",
        "--landing_root", "abfss://landing@acc.dfs.core.windows.net",
        "--checkpoint_root", "abfss://checkpoints@acc.dfs.core.windows.net"]


def test_parses_databricks_named_parameters_style():
    cfg = JobConfig.from_args([
        "--env=dev", "--catalog=telecare_dev", "--landing_root=abfss://l/h", "--checkpoint_root=abfss://c/h",
        "--orchestrator_run_id=adf-123", "--datasets=ehr_patient,call_events", "--max_error_rate=0.1"])
    assert cfg.orchestrator_run_id == "adf-123"
    assert cfg.datasets == ("ehr_patient", "call_events")
    assert cfg.max_error_rate == 0.1


def test_empty_optional_parameters_fall_back_to_defaults():
    cfg = JobConfig.from_args(BASE + ["--max_error_rate=", "--datasets="])
    assert cfg.max_error_rate is None
    assert cfg.datasets == ()


def test_naming_helpers():
    cfg = JobConfig.from_args(BASE)
    assert cfg.table("silver", "call_events") == "telecare_dev.silver.call_events"
    assert cfg.checkpoint("bronze", "ehr_patient").endswith("/dev/bronze/ehr_patient")


@pytest.mark.parametrize("env,catalog", [("dev", "telecare_prd"), ("prd", "telecare_dev")])
def test_guardrail_prevents_cross_environment_writes(env, catalog):
    with pytest.raises(ConfigError, match="guardrail"):
        JobConfig(env=env, catalog=catalog, landing_root="x", checkpoint_root="y")


def test_rejects_unknown_env_and_bad_rate():
    with pytest.raises(ConfigError):
        JobConfig(env="qa", catalog="telecare_qa", landing_root="x", checkpoint_root="y")
    with pytest.raises(ConfigError):
        JobConfig(env="dev", catalog="telecare_dev", landing_root="x", checkpoint_root="y", max_error_rate=2)
