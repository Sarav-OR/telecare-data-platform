"""Run the ingestion locally: simulate ADF's landing, then bronze -> silver with plain Spark.

Uses the *same* transforms, DQ engine and `process_batch` as the Databricks job;
only I/O differs (batch reads instead of Auto Loader, Parquet instead of Delta).
CI uses it to produce silver data for the dbt build, so the whole chain is tested
end-to-end on every pull request without any cloud resources.

    python scripts/run_local_pipeline.py --generated ../work/telecare --out ../work/lakehouse

Step 1 - simulate ADF (what the pipelines do in Azure):
    vendor-drop/*            -> landing/*                  (binary copy, same folders)
    api/partners/<d>/page-*  -> landing/crm/partners/<d>/  (REST copy, one JSON object per line)
    ehr/<table>.csv          -> landing/ehr/<table>/load_date=<d>/*.parquet
                                (daily watermark extract: latest version per key per day)
Step 2 - bronze + silver for all 16 feeds, DQ report.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "data_generator"))

from build_landing import build as build_landing  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from telecare_ingest import datasets  # noqa: E402
from telecare_ingest.config import JobConfig  # noqa: E402
from telecare_ingest.processing import process_batch  # noqa: E402
from telecare_ingest.transforms import add_ingestion_metadata  # noqa: E402


class LocalParquetSink:
    """Stand-in for DeltaSink: `catalog.schema.table` -> <root>/<schema>/<table>/"""

    def __init__(self, spark: SparkSession, root: Path):
        self.spark, self.root = spark, root

    def _path(self, table: str) -> str:
        _, schema, name = table.split(".")
        return str(self.root / schema / name)

    def merge_insert_only(self, df: DataFrame, table: str, keys: tuple[str, ...]) -> None:
        path = self._path(table)
        if Path(path).exists():
            df = df.join(self.spark.read.parquet(path).select(*keys), list(keys), "left_anti")
        df.write.mode("append").parquet(path)

    def append(self, df: DataFrame, table: str, batch_id: int, app_id: str) -> None:
        df.write.mode("append").parquet(self._path(table))


# --------------------------------------------------------------------------- ADF simulation
def simulate_adf(spark: SparkSession, generated: Path, landing: Path, window_start: str,
                 until: str | None = None) -> None:
    """Landing zone as ADF would write it (shared with data_generator/build_landing.py)."""
    build_landing(generated, landing, window_start, until)


# --------------------------------------------------------------------------- bronze + silver
def read_landing(spark: SparkSession, spec, cfg: JobConfig) -> DataFrame:
    path = cfg.landing_path(spec.landing_subpath)
    if spec.file_format == "parquet":
        df = spark.read.parquet(path)
    else:
        reader = spark.read.format(spec.file_format).schema(spec.landing_schema).option("recursiveFileLookup", "true")
        for k, v in spec.reader_options:
            reader = reader.option(k, v)
        if spec.file_format == "csv":
            reader = reader.option("pathGlobFilter", "*.csv")
        df = reader.load(path)
    df = add_ingestion_metadata(df, cfg.orchestrator_run_id).withColumn("_rescued_data", F.lit(None).cast("string"))
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--generated", required=True, help="output folder of data_generator/generate.py")
    ap.add_argument("--out", required=True)
    ap.add_argument("--window-start", default="2026-06-25")
    ap.add_argument("--datasets", default="", help="comma separated subset")
    ap.add_argument("--until", default=None, help="only land data up to this date (YYYY-MM-DD, inclusive)")
    ap.add_argument("--simulate-only", action="store_true", help="stop after building the landing zone")
    args = ap.parse_args()

    generated, out = Path(args.generated).resolve(), Path(args.out).resolve()
    if out.exists():
        shutil.rmtree(out)
    spark = (SparkSession.builder.master("local[4]").appName("telecare-local")
             .config("spark.ui.enabled", "false").config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "8").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    landing = out / "landing"
    print("simulating ADF landing ...")
    simulate_adf(spark, generated, landing, args.window_start, args.until)
    if args.simulate_only:
        files = sum(1 for f in landing.rglob("*") if f.is_file())
        print(f"landing zone ready: {files} files -> {landing}")
        spark.stop()
        return

    cfg = JobConfig(env="dev", catalog="telecare_local", landing_root=str(landing),
                    checkpoint_root=str(out / "_checkpoints"), orchestrator_run_id="local-run")
    sink = LocalParquetSink(spark, out)
    names = tuple(d for d in args.datasets.split(",") if d)
    for spec in datasets.selected(names):
        bronze = read_landing(spark, spec, cfg).cache()
        bronze.count()     # materialise (Spark forbids queries touching only _corrupt_record on raw JSON)
        bronze.write.mode("overwrite").parquet(str(out / "bronze" / spec.name))
        s = process_batch(spark, bronze, 0, spec, cfg, sink)
        silver_rows = spark.read.parquet(str(out / "silver" / spec.name)).count()
        print(f"{spec.name:<24} in={s['rows_in']:>8}  rejected={s['rows_rejected']:>6} "
              f"({s['reject_rate']:.2%})  silver={silver_rows:>8}")
        bronze.unpersist()

    dq = spark.read.parquet(str(out / "ops" / "dq_rule_results"))
    print("\nRules that fired:")
    dq.filter("failed_rows > 0").orderBy("dataset", F.desc("failed_rows")) \
      .select("dataset", "rule_name", "severity", "failed_rows", "failure_rate").show(100, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
