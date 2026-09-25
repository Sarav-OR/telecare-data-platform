"""Landing -> bronze: Auto Loader (availableNow), append-only, one feed after the other.

Feeds run sequentially: on a single-node cluster, 16 concurrent streams would
compete for 4 cores and make run times unpredictable. A failing feed does not
stop the others; the task fails at the end with every failure listed.
"""

from __future__ import annotations

import logging

from telecare_ingest import datasets, io
from telecare_ingest.jobs.common import bootstrap
from telecare_ingest.jobs.setup_catalog import apply_pii_controls

log = logging.getLogger("telecare.bronze")


def main(argv: list[str] | None = None) -> None:
    spark, cfg = bootstrap(argv)
    failures = []
    for spec in datasets.selected(cfg.datasets):
        log.info("bronze %s <- %s", spec.name, cfg.landing_path(spec.landing_subpath))
        try:
            q = io.write_bronze_stream(io.read_landing_stream(spark, spec, cfg), spec, cfg)
            q.awaitTermination()
            log.info("bronze %s finished: %s", spec.name, q.lastProgress)
        except Exception as exc:  # collect all failures, then fail the task once
            log.exception("bronze %s failed", spec.name)
            failures.append(f"{spec.name}: {exc}")
    apply_pii_controls(spark, cfg.catalog, "bronze")          # mask DOB as soon as bronze exists
    if failures:
        raise RuntimeError("bronze ingestion failed -> " + " | ".join(failures))


if __name__ == "__main__":
    main()
