"""Bronze -> silver: incremental Delta stream + foreachBatch (DQ, dedupe, MERGE)."""

from __future__ import annotations

import logging

from telecare_ingest import datasets
from telecare_ingest.io import DeltaSink
from telecare_ingest.jobs.common import bootstrap
from telecare_ingest.processing import RUN_LOG_TABLE, process_batch

log = logging.getLogger("telecare.silver")

RUN_LOG_SCHEMA = ("dataset string, batch_id long, orchestrator_run_id string, rows_in long, "
                  "rows_rejected long, reject_rate double, started_at timestamp, finished_at timestamp")


def main(argv: list[str] | None = None) -> None:
    spark, cfg = bootstrap(argv)
    sink = DeltaSink(spark)

    for spec in datasets.selected(cfg.datasets):
        summaries: list[dict] = []

        def _batch(df, batch_id, _spec=spec, _acc=summaries):
            _acc.append(process_batch(spark, df, batch_id, _spec, cfg, sink))

        source = spark.readStream.table(cfg.table("bronze", spec.name))
        query = (source.writeStream
                 .foreachBatch(_batch)
                 .option("checkpointLocation", cfg.checkpoint("silver", spec.name))
                 .trigger(availableNow=True)
                 .queryName(f"silver_{spec.name}")
                 .start())
        query.awaitTermination()   # sequential: a DQ failure in one dataset stops the run

        if summaries:
            spark.createDataFrame(summaries, RUN_LOG_SCHEMA).write.mode("append") \
                 .saveAsTable(cfg.table("ops", RUN_LOG_TABLE))
        log.info("silver %s: %d micro-batches", spec.name, len(summaries))


if __name__ == "__main__":
    main()
