"""Databricks-specific I/O: Auto Loader readers, Delta writers.

Only this module and the job entry points touch storage; everything else is pure.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from telecare_ingest.config import JobConfig
from telecare_ingest.datasets import DatasetSpec
from telecare_ingest.transforms import add_ingestion_metadata


# ------------------------------------------------------------------ landing -> bronze
def read_landing_stream(spark: SparkSession, spec: DatasetSpec, cfg: JobConfig) -> DataFrame:
    """Incremental file discovery with Auto Loader.

    * explicit all-string schema + `rescue` evolution mode: new vendor fields are
      captured in `_rescued_data` instead of failing the stream or being dropped
    * file notification mode can be switched on per env for high file volumes
    """
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", spec.file_format)
              .option("cloudFiles.schemaLocation", cfg.schema_location(spec.name))
              .option("cloudFiles.schemaEvolutionMode", "rescue")
              .option("rescuedDataColumn", "_rescued_data")
              .option("cloudFiles.includeExistingFiles", "true")
              .option("cloudFiles.maxFilesPerTrigger", "500"))
    if spec.enforce_schema:                       # JSON / CSV: explicit all-string schema
        reader = reader.schema(spec.landing_schema)
    else:                                         # Parquet from ADF: typed schema taken from the files
        reader = reader.option("cloudFiles.inferColumnTypes", "true")
    for k, v in spec.reader_options:
        reader = reader.option(k, v)
    return add_ingestion_metadata(reader.load(cfg.landing_path(spec.landing_subpath)),
                                  cfg.orchestrator_run_id)


def write_bronze_stream(df: DataFrame, spec: DatasetSpec, cfg: JobConfig):
    return (df.writeStream
            .format("delta")
            .option("checkpointLocation", cfg.checkpoint("bronze", spec.name))
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .queryName(f"bronze_{spec.name}")
            .toTable(cfg.table("bronze", spec.name)))


# ------------------------------------------------------------------ Delta sink
class DeltaSink:
    """Production sink used inside foreachBatch."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def merge_insert_only(self, df: DataFrame, table: str, keys: tuple[str, ...]) -> None:
        from delta.tables import DeltaTable  # available on every Databricks runtime

        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (DeltaTable.forName(self.spark, table).alias("t")
         .merge(df.alias("s"), cond)
         .whenNotMatchedInsertAll()
         .execute())

    def append(self, df: DataFrame, table: str, batch_id: int, app_id: str) -> None:
        (df.write.format("delta").mode("append")
         .option("txnAppId", app_id)            # idempotent: a retried micro-batch
         .option("txnVersion", int(batch_id))   # is written at most once
         .option("mergeSchema", "true")
         .saveAsTable(table))
