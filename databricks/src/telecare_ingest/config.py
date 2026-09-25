"""Runtime configuration for Databricks jobs.

Every environment-specific value arrives as a job parameter (set by the Asset
Bundle target and optionally overridden by ADF at run time). Nothing is
hard-coded, and invalid combinations fail fast before any data is touched.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field

VALID_ENVS = ("dev", "tst", "prd")
_CATALOG_RE = re.compile(r"^[a-z][a-z0-9_]{1,62}$")


class ConfigError(ValueError):
    """Raised when job parameters are missing or inconsistent."""


@dataclass(frozen=True)
class JobConfig:
    env: str
    catalog: str
    landing_root: str
    checkpoint_root: str
    orchestrator_run_id: str = "manual"
    datasets: tuple[str, ...] = ()
    max_error_rate: float | None = None           # None -> use the value from dq_rules.yml
    late_threshold_minutes: int = 360
    dq_rules_path: str | None = None               # None -> packaged conf/dq_rules.yml
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    ops_schema: str = "ops"
    extra: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.env not in VALID_ENVS:
            raise ConfigError(f"env must be one of {VALID_ENVS}, got '{self.env}'")
        if not _CATALOG_RE.match(self.catalog):
            raise ConfigError(f"invalid catalog name '{self.catalog}'")
        if self.env == "prd" and not self.catalog.endswith("_prd"):
            raise ConfigError("guardrail: env=prd must write to a *_prd catalog")
        if self.env != "prd" and self.catalog.endswith("_prd"):
            raise ConfigError(f"guardrail: env={self.env} must not write to a production catalog")
        if self.max_error_rate is not None and not 0 <= self.max_error_rate <= 1:
            raise ConfigError("max_error_rate must be between 0 and 1")
        for name in ("landing_root", "checkpoint_root"):
            if not getattr(self, name):
                raise ConfigError(f"{name} is required")

    # -------------------------------------------------------------- naming
    def table(self, layer: str, name: str) -> str:
        schema = {"bronze": self.bronze_schema, "silver": self.silver_schema, "ops": self.ops_schema}[layer]
        return f"{self.catalog}.{schema}.{name}"

    def landing_path(self, subpath: str) -> str:
        return f"{self.landing_root.rstrip('/')}/{subpath}"

    def checkpoint(self, layer: str, dataset: str) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{self.env}/{layer}/{dataset}"

    def schema_location(self, dataset: str) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{self.env}/_schemas/{dataset}"

    # ------------------------------------------------------------- parsing
    @classmethod
    def from_args(cls, argv: list[str] | None = None) -> JobConfig:
        p = argparse.ArgumentParser(prog="telecare-ingest")
        p.add_argument("--env", required=True)
        p.add_argument("--catalog", required=True)
        p.add_argument("--landing_root", "--landing-root", dest="landing_root", required=True)
        p.add_argument("--checkpoint_root", "--checkpoint-root", dest="checkpoint_root", required=True)
        p.add_argument("--orchestrator_run_id", "--orchestrator-run-id", dest="orchestrator_run_id",
                       default="manual")
        p.add_argument("--datasets", default="", help="comma separated subset, empty = all")
        p.add_argument("--max_error_rate", "--max-error-rate", dest="max_error_rate", default="")
        p.add_argument("--late_threshold_minutes", "--late-threshold-minutes",
                       dest="late_threshold_minutes", type=int, default=360)
        p.add_argument("--dq_rules_path", "--dq-rules-path", dest="dq_rules_path", default="")
        a, _unknown = p.parse_known_args(argv)
        return cls(
            env=a.env.strip().lower(),
            catalog=a.catalog.strip(),
            landing_root=a.landing_root.strip(),
            checkpoint_root=a.checkpoint_root.strip(),
            orchestrator_run_id=a.orchestrator_run_id.strip() or "manual",
            datasets=tuple(d.strip() for d in a.datasets.split(",") if d.strip()),
            max_error_rate=float(a.max_error_rate) if str(a.max_error_rate).strip() else None,
            late_threshold_minutes=a.late_threshold_minutes,
            dq_rules_path=a.dq_rules_path.strip() or None,
        )
