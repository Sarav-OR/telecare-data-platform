"""Bronze -> silver micro-batch processing, independent of the storage engine.

`process_batch` is what runs inside Structured Streaming `foreachBatch` on
Databricks (sink = DeltaSink). The same function is used by the local runner
and the tests with a different sink, so what is tested is what is deployed.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession

from telecare_ingest.config import JobConfig
from telecare_ingest.datasets import DatasetSpec
from telecare_ingest.dq import DQEngine
from telecare_ingest.transforms import deduplicate, recover_type_drift

log = logging.getLogger("telecare.processing")

DQ_RESULTS_TABLE = "dq_rule_results"
QUARANTINE_TABLE = "quarantine"
RUN_LOG_TABLE = "batch_run_log"


class Sink(Protocol):
    def merge_insert_only(self, df: DataFrame, table: str, keys: tuple[str, ...]) -> None: ...
    def append(self, df: DataFrame, table: str, batch_id: int, app_id: str) -> None: ...


def process_batch(spark: SparkSession, bronze_batch: DataFrame, batch_id: int,
                  spec: DatasetSpec, cfg: JobConfig, sink: Sink) -> dict:
    """Standardise -> DQ -> quarantine/metrics -> circuit breaker -> dedupe -> merge.

    Order matters: quarantine rows and metrics are written *before* the circuit
    breaker fires so an operator can see why a batch was stopped, and valid rows
    are merged only *after* it passes so a bad feed never half-lands in silver.
    Appends use Delta idempotent writes (txnAppId/txnVersion), so a retried batch
    does not duplicate quarantine or metric rows.
    """
    started = datetime.now(timezone.utc)
    engine = DQEngine.from_yaml(spec.name, cfg.dq_rules_path, cfg.max_error_rate)

    if spec.file_format == "parquet":              # typed extracts: undo producer type drift
        bronze_batch = recover_type_drift(bronze_batch)
    standardized = spec.standardize(bronze_batch, late_threshold_minutes=cfg.late_threshold_minutes)
    standardized = standardized.persist()
    try:
        result = engine.run(standardized)
        app_id = f"{cfg.env}.{spec.name}.silver"

        metrics_df = spark.createDataFrame(
            [dict(m, orchestrator_run_id=cfg.orchestrator_run_id, batch_id=int(batch_id),
                  evaluated_at=started) for m in result.rule_metrics],
            schema="dataset string, rule_name string, rule_type string, severity string, "
                   "failed_rows long, total_rows long, failure_rate double, "
                   "orchestrator_run_id string, batch_id long, evaluated_at timestamp",
        ) if result.rule_metrics else None
        if metrics_df is not None:
            sink.append(metrics_df, cfg.table("ops", DQ_RESULTS_TABLE), batch_id, app_id + ".metrics")

        if result.rejected_rows:
            sink.append(engine.to_quarantine_records(result.quarantined, cfg.orchestrator_run_id, batch_id),
                        cfg.table("ops", QUARANTINE_TABLE), batch_id, app_id + ".quarantine")

        engine.enforce_threshold(result)          # raises -> batch not committed -> job fails

        valid = result.valid
        if "_corrupt_record" in valid.columns:
            valid = valid.drop("_corrupt_record")
        deduped = deduplicate(valid, list(spec.merge_keys), spec.order_by)
        sink.merge_insert_only(deduped, cfg.table("silver", spec.name), spec.merge_keys)

        summary = {
            "dataset": spec.name, "batch_id": int(batch_id), "orchestrator_run_id": cfg.orchestrator_run_id,
            "rows_in": result.total_rows, "rows_rejected": result.rejected_rows,
            "reject_rate": round(result.reject_rate, 6), "started_at": started,
            "finished_at": datetime.now(timezone.utc),
        }
        log.info("silver batch done: %s", summary)
        return summary
    finally:
        standardized.unpersist()
