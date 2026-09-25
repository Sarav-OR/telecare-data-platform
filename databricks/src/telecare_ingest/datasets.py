"""Metadata registry: one entry per source feed (16 feeds from 5 source systems).

Onboarding a new feed = landing schema + standardize function + DQ rules + one
entry here. The bronze and silver jobs simply loop over this registry.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from telecare_ingest import transforms as t
from telecare_ingest.schemas import LANDING


@dataclass(frozen=True)
class DatasetSpec:
    name: str                             # also the bronze and silver table name
    source_system: str
    landing_subpath: str
    file_format: str                      # json | csv | parquet
    standardize: Callable[..., DataFrame]
    merge_keys: tuple[str, ...]
    order_by: str                         # tie-breaker for in-batch de-duplication
    reader_options: tuple[tuple[str, str], ...] = ()
    cluster_by: tuple[str, ...] = ()

    @property
    def landing_schema(self) -> StructType:
        return LANDING[self.name]

    @property
    def enforce_schema(self) -> bool:
        """Text formats get the explicit all-string schema; Parquet carries its own."""
        return self.file_format != "parquet"


_JSON = (("mode", "PERMISSIVE"), ("columnNameOfCorruptRecord", "_corrupt_record"))
_CSV = (("header", "true"), ("sep", ","))
_CSV_CH = (("header", "true"), ("sep", ";"), ("encoding", "UTF-8"))


def _ehr(table: str, keys: tuple[str, ...], fn) -> DatasetSpec:
    return DatasetSpec(f"ehr_{table}", "EHR", f"ehr/{table}", "parquet", fn, keys + ("modified_at",), "ingested_at")


DATASETS: dict[str, DatasetSpec] = {d.name: d for d in [
    DatasetSpec("call_events", "TELEPHONY", "telephony/call_events", "json", t.std_call_events,
                ("event_id",), "ingested_at", _JSON, ("call_id", "event_ts")),
    DatasetSpec("app_events", "APP", "app/app_events", "json", t.std_app_events,
                ("event_id",), "server_ts", _JSON, ("session_id", "server_ts")),
    DatasetSpec("device_measurements", "DEVICES", "devices/measurements", "json", t.std_device_measurements,
                ("measurement_id",), "received_at", _JSON, ("device_serial", "measured_at")),
    DatasetSpec("device_registry", "DEVICES", "devices/device_registry", "csv", t.std_device_registry,
                ("device_serial", "snapshot_date"), "ingested_at", _CSV),
    DatasetSpec("crm_insurers", "CRM", "crm/insurers", "csv", t.std_crm_insurers,
                ("insurer_id", "snapshot_date"), "ingested_at", _CSV_CH),
    DatasetSpec("crm_insurance_plans", "CRM", "crm/insurance_plans", "csv", t.std_crm_insurance_plans,
                ("plan_code", "snapshot_date"), "ingested_at", _CSV_CH),
    DatasetSpec("crm_patient_coverage", "CRM", "crm/patient_coverage", "csv", t.std_crm_patient_coverage,
                ("coverage_id", "snapshot_date"), "ingested_at", _CSV_CH),
    DatasetSpec("crm_partners", "CRM", "crm/partners", "json", t.std_crm_partners,
                ("partner_id", "snapshot_date"), "ingested_at", _JSON),
    _ehr("patient", ("patient_id",), t.std_ehr_patient),
    _ehr("patient_identifier", ("identifier_id",), t.std_ehr_patient_identifier),
    _ehr("staff", ("staff_id",), t.std_ehr_staff),
    _ehr("encounter", ("encounter_id",), t.std_ehr_encounter),
    _ehr("encounter_diagnosis", ("encounter_id", "seq_no"), t.std_ehr_encounter_diagnosis),
    _ehr("prescription", ("prescription_id",), t.std_ehr_prescription),
    _ehr("referral", ("referral_id",), t.std_ehr_referral),
    _ehr("sick_note", ("sick_note_id",), t.std_ehr_sick_note),
]}


def selected(names: tuple[str, ...]) -> list[DatasetSpec]:
    if not names:
        return list(DATASETS.values())
    unknown = set(names) - set(DATASETS)
    if unknown:
        raise KeyError(f"unknown datasets: {sorted(unknown)}; known: {sorted(DATASETS)}")
    return [DATASETS[n] for n in names]
