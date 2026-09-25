"""Declarative, config-driven data quality engine for PySpark.

Design goals
  * rules are data (YAML), not code -> adding a rule is a reviewed config change
  * one pass over the data: every rule becomes a boolean column expression
  * each record carries the names of the rules it broke (`_dq_errors`, `_dq_warnings`)
    so quarantine rows are self-explanatory and can be replayed after a fix
  * rule-level metrics are produced on every batch for observability dashboards
  * a circuit breaker stops the load if the reject rate exceeds a threshold,
    because silently dropping 30 % of a feed is worse than failing loudly
"""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SEVERITIES = ("error", "warn")
RULE_TYPES = ("not_null", "regex", "allowed_values", "range", "expression")


class DataQualityThresholdError(RuntimeError):
    """Reject rate of a batch exceeded the configured circuit-breaker threshold."""


@dataclass(frozen=True)
class Rule:
    name: str
    type: str
    severity: str = "error"
    column: str | None = None
    pattern: str | None = None
    values: tuple[Any, ...] | None = None
    min: float | None = None
    max: float | None = None
    where: str | None = None
    expression: str | None = None

    def __post_init__(self) -> None:
        if self.type not in RULE_TYPES:
            raise ValueError(f"rule '{self.name}': unknown type '{self.type}'")
        if self.severity not in SEVERITIES:
            raise ValueError(f"rule '{self.name}': severity must be one of {SEVERITIES}")
        needs_column = self.type in ("not_null", "regex", "allowed_values", "range")
        if needs_column and not self.column:
            raise ValueError(f"rule '{self.name}': 'column' is required for type {self.type}")
        if self.type == "regex" and not self.pattern:
            raise ValueError(f"rule '{self.name}': 'pattern' is required")
        if self.type == "allowed_values" and not self.values:
            raise ValueError(f"rule '{self.name}': 'values' is required")
        if self.type == "range" and self.min is None and self.max is None:
            raise ValueError(f"rule '{self.name}': 'min' or 'max' is required")
        if self.type == "expression" and not self.expression:
            raise ValueError(f"rule '{self.name}': 'expression' is required")

    def violation(self) -> Column:
        """Boolean column: TRUE when the row violates this rule.

        NULL handling is explicit: only `not_null` fails on NULL. Other rules treat
        NULL as "not applicable" so one missing value is reported exactly once.
        """
        if self.type == "not_null":
            v = F.col(self.column).isNull()
        elif self.type == "regex":
            v = F.col(self.column).isNotNull() & ~F.col(self.column).rlike(self.pattern)
        elif self.type == "allowed_values":
            v = F.col(self.column).isNotNull() & ~F.col(self.column).isin(list(self.values))
        elif self.type == "range":
            c = F.col(self.column)
            out = F.lit(False)
            if self.min is not None:
                out = out | (c < F.lit(self.min))
            if self.max is not None:
                out = out | (c > F.lit(self.max))
            v = c.isNotNull() & out
        else:  # expression: the predicate describes a GOOD row
            v = ~F.coalesce(F.expr(self.expression), F.lit(True))
        if self.where:
            v = F.coalesce(F.expr(self.where), F.lit(False)) & v
        return F.coalesce(v, F.lit(False))

    @classmethod
    def from_dict(cls, d: dict) -> Rule:
        d = dict(d)
        if "values" in d and d["values"] is not None:
            d["values"] = tuple(d["values"])
        return cls(**d)


@dataclass
class DQResult:
    evaluated: DataFrame
    valid: DataFrame
    quarantined: DataFrame
    total_rows: int
    rejected_rows: int
    rule_metrics: list[dict] = field(default_factory=list)

    @property
    def reject_rate(self) -> float:
        return 0.0 if self.total_rows == 0 else self.rejected_rows / self.total_rows


class DQEngine:
    ERRORS_COL = "_dq_errors"
    WARNINGS_COL = "_dq_warnings"

    def __init__(self, dataset: str, rules: list[Rule], key_columns: list[str],
                 max_error_rate: float = 0.05):
        names = [r.name for r in rules]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"duplicate rule names for {dataset}: {sorted(dupes)}")
        self.dataset = dataset
        self.rules = rules
        self.key_columns = key_columns
        self.max_error_rate = max_error_rate

    # ---------------------------------------------------------------- loading
    @classmethod
    def from_yaml(cls, dataset: str, path: str | None = None,
                  max_error_rate_override: float | None = None) -> DQEngine:
        if path:
            text = Path(path).read_text(encoding="utf-8")
        else:
            text = resources.files("telecare_ingest.conf").joinpath("dq_rules.yml").read_text(encoding="utf-8")
        cfg = yaml.safe_load(text)["datasets"]
        if dataset not in cfg:
            raise KeyError(f"no DQ configuration for dataset '{dataset}'")
        ds = cfg[dataset]
        rate = max_error_rate_override if max_error_rate_override is not None else ds.get("max_error_rate", 0.05)
        return cls(dataset, [Rule.from_dict(r) for r in ds["rules"]], ds["key_columns"], float(rate))

    # ------------------------------------------------------------- evaluation
    def _collect_names(self, severity: str) -> Column:
        flagged = [F.when(r.violation(), F.lit(r.name)) for r in self.rules if r.severity == severity]
        if not flagged:
            return F.array().cast("array<string>")
        return F.filter(F.array(*flagged), lambda x: x.isNotNull())

    def evaluate(self, df: DataFrame) -> DataFrame:
        return (df.withColumn(self.ERRORS_COL, self._collect_names("error"))
                  .withColumn(self.WARNINGS_COL, self._collect_names("warn")))

    def rule_metrics(self, evaluated: DataFrame) -> tuple[int, int, list[dict]]:
        """Single aggregation pass -> (total, rejected, per-rule metrics)."""
        aggs = [F.count(F.lit(1)).alias("__total"),
                F.sum(F.when(F.size(self.ERRORS_COL) > 0, 1).otherwise(0)).alias("__rejected")]
        for i, r in enumerate(self.rules):
            col = self.ERRORS_COL if r.severity == "error" else self.WARNINGS_COL
            aggs.append(F.sum(F.when(F.array_contains(col, r.name), 1).otherwise(0)).alias(f"r{i}"))
        row = evaluated.agg(*aggs).collect()[0]
        total, rejected = int(row["__total"] or 0), int(row["__rejected"] or 0)
        metrics = []
        for i, r in enumerate(self.rules):
            failed = int(row[f"r{i}"] or 0)
            metrics.append({
                "dataset": self.dataset, "rule_name": r.name, "rule_type": r.type,
                "severity": r.severity, "failed_rows": failed, "total_rows": total,
                "failure_rate": round(failed / total, 6) if total else 0.0,
            })
        return total, rejected, metrics

    def run(self, df: DataFrame) -> DQResult:
        evaluated = self.evaluate(df)
        total, rejected, metrics = self.rule_metrics(evaluated)
        has_error = F.size(self.ERRORS_COL) > 0
        valid = (evaluated.filter(~has_error)
                 .withColumn("dq_warnings",
                             F.when(F.size(self.WARNINGS_COL) > 0, F.concat_ws(",", self.WARNINGS_COL)))
                 .drop(self.ERRORS_COL, self.WARNINGS_COL))
        quarantined = evaluated.filter(has_error)
        return DQResult(evaluated, valid, quarantined, total, rejected, metrics)

    def enforce_threshold(self, result: DQResult) -> None:
        if result.total_rows and result.reject_rate > self.max_error_rate:
            worst = sorted(result.rule_metrics, key=lambda m: -m["failed_rows"])[:3]
            detail = ", ".join(f"{m['rule_name']}={m['failed_rows']}" for m in worst)
            raise DataQualityThresholdError(
                f"[{self.dataset}] reject rate {result.reject_rate:.2%} exceeds "
                f"threshold {self.max_error_rate:.2%} ({result.rejected_rows}/{result.total_rows}). "
                f"Top failing rules: {detail}")

    # ------------------------------------------------------------- quarantine
    def to_quarantine_records(self, quarantined: DataFrame, run_id: str, batch_id: int) -> DataFrame:
        """Normalise rejected rows of any dataset into one generic quarantine schema."""
        payload_cols = [c for c in quarantined.columns if c not in (self.ERRORS_COL, self.WARNINGS_COL)]
        return quarantined.select(
            F.lit(self.dataset).alias("dataset"),
            F.to_json(F.struct(*[F.col(c) for c in self.key_columns if c in quarantined.columns]))
             .alias("record_key"),
            F.col(self.ERRORS_COL).alias("dq_errors"),
            F.col(self.WARNINGS_COL).alias("dq_warnings"),
            F.to_json(F.struct(*payload_cols)).alias("payload"),
            (F.col("source_file") if "source_file" in quarantined.columns else F.lit(None).cast("string"))
             .alias("source_file"),
            F.lit(run_id).alias("orchestrator_run_id"),
            F.lit(batch_id).cast("long").alias("batch_id"),
            F.current_timestamp().alias("quarantined_at"),
        )
