from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession

from telecare_ingest.config import JobConfig


def bootstrap(argv: list[str] | None = None) -> tuple[SparkSession, JobConfig]:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    logging.getLogger("telecare").info(
        "env=%s catalog=%s run_id=%s datasets=%s", cfg.env, cfg.catalog,
        cfg.orchestrator_run_id, cfg.datasets or "all")
    return spark, cfg
