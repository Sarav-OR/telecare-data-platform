"""Idempotent setup: schemas, silver/ops tables with explicit contracts, PII controls.

Silver table DDL is *derived from the standardize functions* (applied to an empty
DataFrame with the landing schema), so the table contract and the transformation
can never drift apart - and it is still created up-front, reviewed and versioned,
instead of being inferred by the first write.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from telecare_ingest import datasets
from telecare_ingest.datasets import DatasetSpec
from telecare_ingest.jobs.common import bootstrap

log = logging.getLogger("telecare.setup")

TABLE_PROPS = ("TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', "
               "'delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')")
PII_GROUP = "pii_readers"

META = [StructField("_source_file", StringType()), StructField("_file_modified_at", TimestampType()),
        StructField("_ingested_at", TimestampType()), StructField("_orchestrator_run_id", StringType()),
        StructField("_rescued_data", StringType())]


def silver_schema(spark: SparkSession, spec: DatasetSpec) -> StructType:
    empty = spark.createDataFrame([], StructType(list(spec.landing_schema.fields) + META))
    out = spec.standardize(empty, late_threshold_minutes=360)
    if "_corrupt_record" in out.columns:
        out = out.drop("_corrupt_record")
    return StructType(out.schema.fields + [StructField("dq_warnings", StringType())])


def silver_ddl(spark: SparkSession, catalog: str, spec: DatasetSpec) -> str:
    cols = ",\n  ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in silver_schema(spark, spec).fields)
    cluster = f"CLUSTER BY ({', '.join(spec.cluster_by)})" if spec.cluster_by else ""
    return (f"CREATE TABLE IF NOT EXISTS {catalog}.silver.{spec.name} (\n  {cols}\n) {cluster}\n"
            f"COMMENT 'Silver: {spec.source_system} feed {spec.name} - typed, validated, de-duplicated'\n{TABLE_PROPS}")


def ops_ddl(catalog: str) -> list[str]:
    o = f"{catalog}.ops"
    return [
        f"""CREATE TABLE IF NOT EXISTS {o}.dq_rule_results (
              dataset STRING, rule_name STRING, rule_type STRING, severity STRING, failed_rows BIGINT,
              total_rows BIGINT, failure_rate DOUBLE, orchestrator_run_id STRING, batch_id BIGINT,
              evaluated_at TIMESTAMP) COMMENT 'Per-batch, per-rule data quality metrics'""",
        f"""CREATE TABLE IF NOT EXISTS {o}.quarantine (
              dataset STRING, record_key STRING, dq_errors ARRAY<STRING>, dq_warnings ARRAY<STRING>,
              payload STRING, source_file STRING, orchestrator_run_id STRING, batch_id BIGINT,
              quarantined_at TIMESTAMP) COMMENT 'Rejected records with reasons; replayable after a fix'""",
        f"""CREATE TABLE IF NOT EXISTS {o}.batch_run_log (
              dataset STRING, batch_id BIGINT, orchestrator_run_id STRING, rows_in BIGINT,
              rows_rejected BIGINT, reject_rate DOUBLE, started_at TIMESTAMP, finished_at TIMESTAMP)
            COMMENT 'One row per processed silver micro-batch'""",
    ]


def pii_function_ddl(catalog: str) -> str:
    """Only members of `pii_readers` see the date of birth; everyone else gets NULL.

    The identities that *run* the pipelines (ingestion job, dbt) must be members, because
    they need the real value to compute ages; analysts are not, so they never see it.
    """
    return (f"CREATE OR REPLACE FUNCTION {catalog}.ops.mask_date_of_birth(dob DATE) RETURNS DATE "
            f"RETURN CASE WHEN is_account_group_member('{PII_GROUP}') THEN dob ELSE NULL END")


def apply_pii_controls(spark: SparkSession, catalog: str, layer: str) -> None:
    """Tag and mask the date of birth wherever it is stored (bronze and silver)."""
    table = f"{catalog}.{layer}.ehr_patient"
    if not spark.catalog.tableExists(table):
        return
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN date_of_birth SET MASK {catalog}.ops.mask_date_of_birth")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN date_of_birth SET TAGS ('pii' = 'date_of_birth', "
              f"'data_classification' = 'restricted')")
    log.info("PII controls applied on %s.date_of_birth", table)


def main(argv: list[str] | None = None) -> None:
    spark, cfg = bootstrap(argv)
    c = cfg.catalog
    for schema, comment in [("bronze", "Raw, append-only copy of landing files"),
                            ("silver", "Typed, validated, de-duplicated source data"),
                            ("ops", "Data quality results, quarantine, run logs, PII functions")]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c}.{schema} COMMENT '{comment}'")
    for stmt in ops_ddl(c) + [pii_function_ddl(c)]:
        spark.sql(stmt)
    for spec in datasets.selected(cfg.datasets):
        log.info("ensuring silver table %s", spec.name)
        spark.sql(silver_ddl(spark, c, spec))
    apply_pii_controls(spark, c, "silver")
    apply_pii_controls(spark, c, "bronze")


if __name__ == "__main__":
    main()
