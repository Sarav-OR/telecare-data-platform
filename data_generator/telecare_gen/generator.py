"""TeleCare CH synthetic data generator - five source systems, one consistent world.

The generator simulates people contacting a telemedicine provider and follows each
contact through every system it touches:

    telephony events / app events  ->  EHR encounter (+ diagnoses, prescriptions,
    referrals, sick notes)  ->  pharmacy device measurements (Connect encounters)

while CRM (insurers, plans, coverage, partner directory) and the device registry
change slowly in the background. All systems share the same underlying people,
but each identifies them differently (phone hash, app user id, EHR patient id),
exactly the integration problem a real platform has to solve.
"""

from __future__ import annotations

import bisect
import hashlib
import itertools
import json
import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from . import reference as ref
from .writers import (
    CsvTableWriter,
    HourlyJsonlWriter,
    ehr_local,
    iso_z,
    swiss_date,
    write_api_pages,
    write_snapshot_csv,
)

UTC = timezone.utc
ZURICH = ZoneInfo("Europe/Zurich")
PHONE_SALT = "telecare-demo-salt"      # in reality: a secret held by the telephony vendor

EHR_TABLES = {
    "patient": ["patient_id", "date_of_birth", "sex", "canton", "postal_code", "preferred_language",
                "is_deleted", "created_at", "modified_at"],
    "patient_identifier": ["identifier_id", "patient_id", "identifier_type", "identifier_value", "verified_at",
                           "is_deleted", "created_at", "modified_at"],
    "staff": ["staff_id", "staff_login", "role", "specialty", "team", "languages", "employment_pct",
              "hired_at", "left_at", "is_deleted", "created_at", "modified_at"],
    "encounter": ["encounter_id", "patient_id", "staff_id", "channel", "service_line", "contact_system",
                  "contact_ref", "started_at", "ended_at", "triage_level", "disposition_code", "plan_code",
                  "tariff_amount_chf", "status", "is_deleted", "created_at", "modified_at"],
    "encounter_diagnosis": ["encounter_id", "seq_no", "icd10_code", "diagnosis_role", "is_deleted",
                            "created_at", "modified_at"],
    "prescription": ["prescription_id", "encounter_id", "atc_code", "quantity", "pharmacy_partner_id",
                     "issued_at", "is_deleted", "created_at", "modified_at"],
    "referral": ["referral_id", "encounter_id", "partner_id", "referral_type", "urgency", "issued_at",
                 "is_deleted", "created_at", "modified_at"],
    "sick_note": ["sick_note_id", "encounter_id", "incapacity_pct", "days", "valid_from", "issued_at",
                  "is_deleted", "created_at", "modified_at"],
}


# ============================================================================ entities
@dataclass
class Person:
    key: int
    dob: date
    sex: str
    canton: str
    postal_code: str
    language: str
    registered: bool
    patient_id: str | None = None
    phones: list[str] = field(default_factory=list)          # phone hashes this person calls from
    app_user_id: str | None = None
    plan_code: str | None = None
    weight: float = 1.0

    def age_on(self, d: date) -> int:
        return d.year - self.dob.year - ((d.month, d.day) < (self.dob.month, self.dob.day))


@dataclass
class Staff:
    staff_id: str
    login: str
    role: str
    specialty: str | None
    team: str
    languages: list[str]
    employment_pct: int
    hired_at: datetime
    left_at: datetime | None = None

    def active(self, t: datetime) -> bool:
        return self.hired_at <= t and (self.left_at is None or t < self.left_at)


@dataclass
class Partner:
    partner_id: str
    partner_type: str
    name: str
    canton: str
    city: str
    postal_code: str
    connect_enabled: bool
    active: bool
    opened_on: date
    deactivated_on: date | None = None


@dataclass
class Device:
    serial: str
    model: str
    firmware: str
    partner_id: str
    status: str
    installed_on: date


# ============================================================================ generator
class TeleCareGenerator:
    def __init__(self, out: Path, start: date, days: int, n_patients: int, contacts_per_day: int,
                 seed: int, issue_scale: float, api_base_url: str):
        self.r = random.Random(seed)
        self.out = out
        self.start, self.days = start, days
        self.end = start + timedelta(days=days)
        self.horizon = datetime(self.end.year, self.end.month, self.end.day, tzinfo=ZURICH).astimezone(UTC)
        self.n_patients = n_patients
        self.contacts_per_day = contacts_per_day
        self.s = issue_scale
        self.api_base_url = api_base_url.rstrip("/")

        self.icd = {r["icd10_code"]: r for r in ref.load_seed("icd10_codes")}
        self.icd_by_cat: dict[str, list[str]] = {}
        for code, row in self.icd.items():
            self.icd_by_cat.setdefault(row["symptom_category"], []).append(code)
        self.cantons = ref.load_seed("cantons")
        self.service_lines = {r["service_line_code"]: r for r in ref.load_seed("service_lines")}
        self.holidays = {date.fromisoformat(r["holiday_date"]) for r in ref.load_seed("swiss_public_holidays")
                         if r["scope"] == "national"}

        self.people: list[Person] = []
        self.patients_by_id: dict[str, Person] = {}
        self.staff: list[Staff] = []
        self.partners: list[Partner] = []
        self.devices: list[Device] = []
        self.coverage: list[dict] = []
        self.plans: list[dict] = []
        self.insurers: list[dict] = []
        self._seq: dict[str, int] = {}
        self.stats: dict[str, int] = {}

        vd = out / "vendor-drop"
        self.w_calls = HourlyJsonlWriter(vd, "telephony/call_events", "call_events")
        self.w_app = HourlyJsonlWriter(vd, "app/app_events", "app_events")
        self.w_meas = HourlyJsonlWriter(vd, "devices/measurements", "measurements")
        self.ehr = {t: CsvTableWriter(out / "ehr" / f"{t}.csv", cols) for t, cols in EHR_TABLES.items()}

    # ------------------------------------------------------------------ helpers
    def _next(self, kind: str) -> int:
        self._seq[kind] = self._seq.get(kind, 0) + 1
        return self._seq[kind]

    def _count(self, key: str, n: int = 1) -> None:
        self.stats[key] = self.stats.get(key, 0) + n

    def _uuid(self) -> str:
        h = f"{self.r.getrandbits(128):032x}"
        return f"{h[:8]}-{h[8:12]}-4{h[13:16]}-a{h[17:20]}-{h[20:32]}"

    def _phone_hash(self) -> str:
        number = f"+417{self.r.randint(5, 9)}{self.r.randint(0, 9999999):07d}"
        return hashlib.sha256((PHONE_SALT + number).encode()).hexdigest()

    def _rand_day(self, lo: int, margin: int) -> int:
        """Random day offset in [lo, days - margin], safe for short test windows."""
        hi = max(lo, self.days - margin)
        return self.r.randint(min(lo, self.days - 1), min(hi, self.days - 1))

    def _choice_w(self, items, weights):
        return self.r.choices(items, weights=weights, k=1)[0]

    def _local_dt(self, d: date, hour: int, minute: int, second: int) -> datetime:
        return datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=ZURICH).astimezone(UTC)

    def _day_start_utc(self, d: date) -> datetime:
        return datetime(d.year, d.month, d.day, tzinfo=ZURICH).astimezone(UTC)

    # ================================================================== master data
    def build_master_data(self) -> None:
        self._build_insurers_and_plans()
        self._build_people()
        self._build_staff()
        self._build_partners_and_devices()

    def _build_insurers_and_plans(self) -> None:
        for ins_id, name in ref.INSURERS:
            self.insurers.append({"insurer_id": ins_id, "insurer_name": name, "is_active": "true",
                                  "valid_since": date(2010, 1, 1)})
            for suffix, model, _w, telmed, discount in ref.PLAN_MODELS:
                self.plans.append({"plan_code": f"{ins_id}-{suffix}", "insurer_id": ins_id,
                                   "plan_name": f"{name} {model.replace('_', ' ').title()}",
                                   "model": model, "telmed_first_contact_required": str(telmed).lower(),
                                   "premium_discount_pct": discount})
        self.plan_weights = {p["plan_code"]: next(w for s, _m, w, _t, _d in ref.PLAN_MODELS
                                                   if p["plan_code"].endswith(s)) for p in self.plans}

    def _pick_plan(self) -> str:
        codes = list(self.plan_weights)
        return self._choice_w(codes, [self.plan_weights[c] for c in codes])

    def _new_person(self, registered: bool, created: datetime) -> Person:
        r = self.r
        canton = self._choice_w([c["canton_code"] for c in self.cantons],
                                [float(c["population_weight"]) for c in self.cantons])
        lang_region = next(c["language_region"] for c in self.cantons if c["canton_code"] == canton)
        language = "en" if canton in ref.EXPAT_CANTONS and r.random() < 0.12 else lang_region
        age = self._choice_w([(0, 15), (16, 30), (31, 45), (46, 65), (66, 92)], [20, 22, 25, 22, 11])
        years = r.randint(*age)
        dob = self.start - timedelta(days=years * 365 + r.randint(0, 364))
        lo, hi = ref.POSTAL_RANGES[canton]
        p = Person(key=len(self.people), dob=dob, sex=self._choice_w(["F", "M", "X"], [51, 48.5, 0.5]),
                   canton=canton, postal_code=str(r.randint(lo, hi)), language=language,
                   registered=registered, plan_code=self._pick_plan())
        model = p.plan_code.split("-")[1]
        p.weight = (2.0 if model in ("TEL", "HMO") else 1.0) * (1.5 if years <= 15 else 1.3 if years >= 66 else 1.0)
        self.people.append(p)
        if registered:
            self._register(p, created)
        return p

    def _register(self, p: Person, at: datetime) -> None:
        """Create the EHR patient (and coverage in CRM) the first time a person is registered."""
        p.registered = True
        p.patient_id = f"P{self._next('patient'):07d}"
        self.patients_by_id[p.patient_id] = p
        self._write_patient(p, created=at, modified=at)
        for ph in p.phones:
            self._write_identifier(p, "PHONE_HASH", ph, at)
        if p.app_user_id:
            self._write_identifier(p, "APP_USER_ID", p.app_user_id, at)
        self.coverage.append({"coverage_id": f"COV{self._next('coverage'):08d}", "patient_id": p.patient_id,
                              "plan_code": p.plan_code, "valid_from": date(at.year, 1, 1), "valid_to": None,
                              "created_on": at.astimezone(ZURICH).date()})
        self._count("patients_registered")

    def _write_patient(self, p: Person, created: datetime, modified: datetime, deleted: bool = False) -> None:
        self.ehr["patient"].write({
            "patient_id": p.patient_id, "date_of_birth": p.dob.isoformat(), "sex": p.sex, "canton": p.canton,
            "postal_code": p.postal_code, "preferred_language": p.language, "is_deleted": int(deleted),
            "created_at": ehr_local(created), "modified_at": ehr_local(modified)})

    def _write_identifier(self, p: Person, id_type: str, value: str, at: datetime) -> None:
        self.ehr["patient_identifier"].write({
            "identifier_id": f"ID{self._next('identifier'):08d}", "patient_id": p.patient_id,
            "identifier_type": id_type, "identifier_value": value, "verified_at": ehr_local(at),
            "is_deleted": 0, "created_at": ehr_local(at), "modified_at": ehr_local(at)})

    def _build_people(self) -> None:
        r = self.r
        adults_by_canton: dict[str, list[Person]] = {}
        before = self._day_start_utc(self.start)
        # create everybody first (unregistered), then register -> identifiers exist before registration
        for _ in range(self.n_patients):
            created = before - timedelta(days=r.randint(1, 3 * 365), minutes=r.randint(0, 1439))
            p = self._new_person(registered=False, created=created)
            p._created = created                                    # type: ignore[attr-defined]
            if p.age_on(self.start) >= 18:
                adults_by_canton.setdefault(p.canton, []).append(p)
        for p in self.people:
            if p.age_on(self.start) <= 15:
                parents = adults_by_canton.get(p.canton) or next(iter(adults_by_canton.values()))
                if r.random() < 0.85:
                    parent = r.choice(parents)
                    if not parent.phones:
                        parent.phones.append(self._phone_hash())
                    p.phones.append(parent.phones[0])                  # children call from a parent's phone
            else:
                if r.random() < 0.85:
                    p.phones.append(self._phone_hash())
                if r.random() < 0.10:
                    p.phones.append(self._phone_hash())
                if r.random() < 0.65 and p.age_on(self.start) < 80:
                    p.app_user_id = f"u_{r.getrandbits(48):012x}"
        for p in sorted(self.people, key=lambda x: x._created):       # type: ignore[attr-defined]
            self._register(p, p._created)                            # type: ignore[attr-defined]
        # patients moving canton during the window -> SCD2 in dim_patient
        for p in r.sample(self.people, k=int(len(self.people) * 0.015)):
            at = self._local_dt(self.start + timedelta(days=self._rand_day(5, 2)), r.randint(8, 17),
                                r.randint(0, 59), r.randint(0, 59))
            new_c = r.choice([c["canton_code"] for c in self.cantons if c["canton_code"] != p.canton])
            p._pending_move = (at, new_c)                            # type: ignore[attr-defined]
            self._count("patient_moves")

    def _apply_moves(self, day_end: datetime) -> None:
        for p in self.people:
            move = getattr(p, "_pending_move", None)
            if move and move[0] < day_end:
                at, canton = move
                lo, hi = ref.POSTAL_RANGES[canton]
                p.canton, p.postal_code = canton, str(self.r.randint(lo, hi))
                self._write_patient(p, created=p._created, modified=at)   # type: ignore[attr-defined]
                del p._pending_move                                       # type: ignore[attr-defined]

    def _build_staff(self) -> None:
        r = self.r
        long_ago = self._day_start_utc(self.start) - timedelta(days=900)
        mix = [("PHYSICIAN", "GENERAL_MEDICINE", "GENERAL", 90), ("PHYSICIAN", "INTERNAL_MEDICINE", "GENERAL", 20),
               ("PHYSICIAN", "PAEDIATRICS", "PAEDIATRICS", 30), ("PHYSICIAN", "EMERGENCY_MEDICINE", "EMERGENCY", 20),
               ("MEDICAL_ASSISTANT", None, "TRIAGE", 120)]
        for role, specialty, team, n in mix:
            for _ in range(n):
                i = self._next("staff")
                langs = [self._choice_w(["de", "fr", "it"], [65, 27, 8])]
                if r.random() < 0.7:
                    langs.append("en")
                if r.random() < 0.3 and "de" not in langs:
                    langs.append("de")
                hired = long_ago + timedelta(days=r.randint(0, 850), hours=r.randint(0, 23))
                self.staff.append(Staff(f"S{i:05d}", f"tc{i:04d}", role, specialty, team, langs,
                                        r.choice([40, 50, 60, 80, 80, 100, 100]), hired))
        # a few new hires during the window
        for _ in range(6):
            i = self._next("staff")
            hired = self._local_dt(self.start + timedelta(days=self._rand_day(10, 10)), 8, 0, 0)
            self.staff.append(Staff(f"S{i:05d}", f"tc{i:04d}", "PHYSICIAN", "GENERAL_MEDICINE", "GENERAL",
                                    ["de", "en"], 80, hired))
        for s in self.staff:
            self._write_staff(s, created=s.hired_at, modified=s.hired_at)
        # changes during the window: team transfers, employment %, leavers -> SCD2 dim_staff
        self._staff_changes = []
        for s in r.sample([x for x in self.staff if x.role == "PHYSICIAN"], k=12):
            at = self._local_dt(self.start + timedelta(days=self._rand_day(7, 5)), 9, 0, 0)
            kind = r.choice(["pct", "pct", "leave"])
            self._staff_changes.append((at, s, kind))

    def _write_staff(self, s: Staff, created: datetime, modified: datetime) -> None:
        self.ehr["staff"].write({
            "staff_id": s.staff_id, "staff_login": s.login, "role": s.role, "specialty": s.specialty or "",
            "team": s.team, "languages": ",".join(s.languages), "employment_pct": s.employment_pct,
            "hired_at": ehr_local(s.hired_at), "left_at": ehr_local(s.left_at), "is_deleted": 0,
            "created_at": ehr_local(created), "modified_at": ehr_local(modified)})

    def _apply_staff_changes(self, day_end: datetime) -> None:
        remaining = []
        for at, s, kind in self._staff_changes:
            if at >= day_end:
                remaining.append((at, s, kind))
                continue
            if kind == "pct":
                s.employment_pct = self.r.choice([p for p in (50, 60, 80, 100) if p != s.employment_pct])
            else:
                s.left_at = at
            self._write_staff(s, created=s.hired_at, modified=at)
            self._count("staff_changes")
        self._staff_changes = remaining

    def _build_partners_and_devices(self) -> None:
        r = self.r
        mix = [("PHARMACY", 900), ("GP_PRACTICE", 400), ("SPECIALIST", 150), ("HOSPITAL_ED", 50)]
        for ptype, n in mix:
            for _ in range(n):
                i = self._next("partner")
                canton = self._choice_w([c["canton_code"] for c in self.cantons],
                                        [float(c["population_weight"]) for c in self.cantons])
                lang = next(c["language_region"] for c in self.cantons if c["canton_code"] == canton)
                city = ref.CANTON_CAPITALS[canton]
                name = f"{ref.PARTNER_NAME_PARTS[lang][ptype]} {r.choice(ref.PARTNER_SUFFIXES)} {city}"
                lo, hi = ref.POSTAL_RANGES[canton]
                self.partners.append(Partner(f"PRT{i:05d}", ptype, name, canton, city, str(r.randint(lo, hi)),
                                             connect_enabled=(ptype == "PHARMACY" and r.random() < 0.22),
                                             active=True,
                                             opened_on=date(2015, 1, 1) + timedelta(days=r.randint(0, 3000))))
        for p in self.partners:
            if p.connect_enabled:
                self._install_device(p, self.start - timedelta(days=r.randint(30, 400)))
        self.partner_events = []
        for p in r.sample([x for x in self.partners if x.partner_type == "PHARMACY" and not x.connect_enabled], k=30):
            self.partner_events.append((self.start + timedelta(days=42), p, "connect"))
        for p in r.sample(self.partners, k=12):
            self.partner_events.append((self.start + timedelta(days=self._rand_day(10, 5)), p, "deactivate"))
        self.device_events = [(self.start + timedelta(days=56), d, "firmware") for d in self.devices
                              if d.model == "VitalCheck Station 3"]
        for d in r.sample(self.devices, k=3):
            day = self._rand_day(20, 10)
            self.device_events += [(self.start + timedelta(days=day), d, "repair"),
                                   (self.start + timedelta(days=day + 7), d, "back")]

    def _install_device(self, partner: Partner, on: date) -> None:
        model = self._choice_w(list(ref.DEVICE_MODELS), [35, 65])
        fw = "1.9.3" if model.endswith("2") else "2.5.0"
        self.devices.append(Device(f"TCD-{self._next('device'):06d}", model, fw, partner.partner_id, "ACTIVE", on))

    def _apply_partner_device_events(self, d: date) -> None:
        for on, p, kind in self.partner_events:
            if on == d:
                if kind == "connect":
                    p.connect_enabled = True
                    self._install_device(p, d)
                else:
                    p.active, p.deactivated_on = False, d
        for on, dev, kind in self.device_events:
            if on == d:
                if kind == "firmware":
                    dev.firmware = "2.6.0"
                elif kind == "repair":
                    dev.status = "IN_REPAIR"
                else:
                    dev.status = "ACTIVE"

    # ================================================================== snapshots
    def write_snapshots(self, d: date) -> None:
        """Weekly CRM exports (Swiss CSV: ';' and dd.mm.yyyy), device registry and partner API pages."""
        vd = self.out / "vendor-drop"
        tag = d.isoformat()
        write_snapshot_csv(vd / "crm" / "insurers" / tag / "insurers.csv",
                           [dict(i, valid_since=swiss_date(i["valid_since"])) for i in self.insurers],
                           ["insurer_id", "insurer_name", "is_active", "valid_since"], delimiter=";")
        write_snapshot_csv(vd / "crm" / "insurance_plans" / tag / "insurance_plans.csv", self.plans,
                           ["plan_code", "insurer_id", "plan_name", "model", "telmed_first_contact_required",
                            "premium_discount_pct"], delimiter=";")
        cov = [c for c in self.coverage if c["created_on"] <= d]
        rows = [{**c, "valid_from": swiss_date(c["valid_from"]), "valid_to": swiss_date(c["valid_to"])} for c in cov]
        if self.s > 0 and rows:                                      # duplicate export row
            rows.append(dict(self.r.choice(rows)))
        write_snapshot_csv(vd / "crm" / "patient_coverage" / tag / "patient_coverage.csv", rows,
                           ["coverage_id", "patient_id", "plan_code", "valid_from", "valid_to"], delimiter=";")
        write_snapshot_csv(vd / "devices" / "device_registry" / tag / "device_registry.csv",
                           [{"device_serial": x.serial, "model": x.model, "manufacturer": "Helvetic MedTech AG",
                             "firmware_version": x.firmware, "partner_id": x.partner_id, "status": x.status,
                             "installed_on": x.installed_on.isoformat()} for x in self.devices if x.installed_on <= d],
                           ["device_serial", "model", "manufacturer", "firmware_version", "partner_id", "status",
                            "installed_on"])
        items = [{"partnerId": p.partner_id, "type": p.partner_type, "name": p.name,
                  "address": {"city": p.city, "postalCode": p.postal_code, "canton": p.canton},
                  "connectEnabled": p.connect_enabled, "active": p.active,
                  "openedOn": p.opened_on.isoformat(),
                  "deactivatedOn": p.deactivated_on.isoformat() if p.deactivated_on else None}
                 for p in self.partners]
        write_api_pages(self.out / "api" / "partners" / tag, items, 200, d, f"{self.api_base_url}/partners/{tag}")
        self._count("snapshots")

    def _plan_changes(self, d: date) -> None:
        """1 Aug: some patients switch plan (e.g. after moving) -> effectivity / SCD2 in CRM."""
        if d != date(d.year, 8, 1):
            return
        active = [c for c in self.coverage if c["valid_to"] is None]
        for c in self.r.sample(active, k=int(len(active) * 0.02)):
            p = self.patients_by_id[c["patient_id"]]
            new_plan = self._pick_plan()
            overlap = self.s > 0 and self.r.random() < 0.08            # data issue: overlapping periods
            c["valid_to"] = d + timedelta(days=3) if overlap else d - timedelta(days=1)
            p.plan_code = new_plan
            self.coverage.append({"coverage_id": f"COV{self._next('coverage'):08d}", "patient_id": p.patient_id,
                                  "plan_code": new_plan, "valid_from": d, "valid_to": None, "created_on": d})
            self._count("plan_changes")

    # ================================================================== activity
    def run(self) -> None:
        self.build_master_data()
        for i in range(self.days):
            d = self.start + timedelta(days=i)
            day_end = self._day_start_utc(d + timedelta(days=1))
            self._apply_partner_device_events(d)
            self._plan_changes(d)
            if i == 0 or d.weekday() == 0:
                self.write_snapshots(d)
            self._simulate_day(d)
            self._apply_moves(day_end)
            self._apply_staff_changes(day_end)
            for w in (self.w_calls, self.w_app, self.w_meas):
                w.flush()
        for w in self.ehr.values():
            w.close()

    def _simulate_day(self, d: date) -> None:
        r = self.r
        weekend = d.weekday() >= 5 or d in self.holidays
        n = int(self.contacts_per_day * ref.WEEKDAY_FACTOR[d.weekday()] * r.uniform(0.94, 1.06))
        hour_w = ref.HOUR_WEIGHTS_WEEKEND if weekend else ref.HOUR_WEIGHTS_WEEKDAY
        avg_w = sum(hour_w) / 24
        population = list(self.people)                       # people known at the start of the day
        cum = list(itertools.accumulate(p.weight for p in population))
        for _ in range(n):
            hour = self._choice_w(range(24), hour_w)
            t0 = self._local_dt(d, hour, r.randint(0, 59), r.randint(0, 59))
            load = hour_w[hour] / avg_w
            roll = r.random()
            if roll < 0.87:
                person = population[bisect.bisect_left(cum, r.random() * cum[-1])]
            elif roll < 0.92:
                person = self._new_person(registered=False, created=t0)       # first-time contact (5 %)
                if person.age_on(d) <= 15 or r.random() < 0.9:
                    person.phones.append(self._phone_hash())
                if person.age_on(d) > 15 and r.random() < 0.35:
                    person.app_user_id = f"u_{r.getrandbits(48):012x}"
            else:
                person = None                                                # admin / anonymous caller
            self._simulate_contact(d, t0, person, load, weekend)

    def _service_line(self, person: Person | None, d: date, t0: datetime) -> str:
        r = self.r
        if person is None:
            return "MED_CENTER"
        night = not (7 <= t0.astimezone(ZURICH).hour < 19)
        if person.age_on(d) <= 15 and r.random() < 0.8:
            return "KIDS_LINE"
        if r.random() < (0.15 if night else 0.06):
            return "EMERGENCY"
        if r.random() < 0.025:
            return "PHARMACY_CONNECT"
        return "MED_CENTER"

    def _simulate_contact(self, d: date, t0: datetime, person: Person | None, load: float, weekend: bool) -> None:
        r = self.r
        sl = self._service_line(person, d, t0)
        use_app = sl == "PHARMACY_CONNECT" or (
            person is not None and person.app_user_id is not None and sl not in ("EMERGENCY",) and r.random() < 0.85)
        self._count("contacts")
        if use_app:
            self._app_session(d, t0, person, sl, load, weekend)
        else:
            self._phone_call(d, t0, person, sl, load, weekend)

    # ------------------------------------------------------------------ telephony
    def _emit(self, writer: HourlyJsonlWriter, ship: datetime, rec: dict, dup_rate: float, dup_key: str) -> None:
        """Write one event; occasionally re-deliver it or corrupt a line (real feed behaviour)."""
        writer.add(ship, rec)
        if self.r.random() < dup_rate * self.s:
            writer.add(min(ship + timedelta(minutes=self.r.randint(1, 30)), self.horizon - timedelta(seconds=1)), rec)
            self._count(dup_key)
        if self.r.random() < 0.0003 * self.s:
            line = json.dumps(rec, separators=(",", ":"))
            writer.add(ship, line[: len(line) // 2])                      # truncated / malformed line
            self._count("injected_malformed_lines")

    def _emit_call(self, ev: dict, ship: datetime) -> None:
        self._emit(self.w_calls, ship, ev, 0.01, "injected_call_duplicates")

    def _phone_call(self, d: date, t0: datetime, person: Person | None, sl: str, load: float, weekend: bool) -> None:
        r = self.r
        call_id = f"CALL-{self._next('call'):09d}"
        caller = (r.choice(person.phones) if person and person.phones else self._phone_hash())
        lang = person.language if person else self._choice_w(["de", "fr", "it", "en"], [60, 25, 7, 8])
        if person is None:
            queue = "Q_ADMIN"
        elif sl == "KIDS_LINE":
            queue = "Q_KIDS_FR" if lang == "fr" else "Q_KIDS_DE"
        elif sl == "EMERGENCY":
            queue = "Q_EMERG"
        else:
            queue = {"de": "Q_GEN_DE", "fr": "Q_GEN_FR", "it": "Q_GEN_IT"}.get(lang, "Q_GEN_EN")
        if r.random() < 0.002 * self.s:
            queue = "Q_LEGACY_99"                                             # unknown queue code
        dialled = self.service_lines[sl]["dialled_line"]
        seq = 0

        def ev(event_type: str, ts: datetime, agent: str | None = None) -> dict:
            nonlocal seq
            seq += 1
            return {"event_id": self._uuid(), "call_id": call_id, "sequence_no": seq, "event_type": event_type,
                    "event_ts": iso_z(ts), "dialled_line": dialled, "queue_code": queue, "agent_login": agent,
                    "caller_phone_hash": caller, "ivr_language": lang}

        events: list[tuple[dict, datetime]] = [(ev("OFFERED", t0), t0)]
        t_q = t0 + timedelta(seconds=r.randint(15, 60))
        events.append((ev("QUEUED", t_q), t_q))
        mean_wait = (8 if sl == "EMERGENCY" else 25 if sl == "KIDS_LINE" else 40) * load ** 1.6
        wait = r.expovariate(1 / max(mean_wait, 3))
        p_abandon = min(0.35, wait / 600) + 0.02
        encounter_start = None
        if r.random() < 0.03:
            t_cb = t_q + timedelta(seconds=min(wait, 90))
            events.append((ev("CALLBACK_REQUESTED", t_cb), t_cb))
            events.append((ev("ENDED", t_cb + timedelta(seconds=5)), t_cb + timedelta(seconds=5)))
            if person is not None:
                encounter_start = t_cb + timedelta(minutes=r.randint(20, 90))       # callback consultation
        elif r.random() < p_abandon:
            t_ab = t_q + timedelta(seconds=wait * r.uniform(0.3, 1.0))
            events.append((ev("ABANDONED", t_ab), t_ab))
        else:
            t_ans = t_q + timedelta(seconds=wait)
            ma = self._pick_staff("TRIAGE", lang, t_ans)
            events.append((ev("ANSWERED", t_ans, ma.login if ma else None), t_ans))
            triage_s = r.randint(60, 240)
            if person is not None and r.random() < 0.85:
                t_tr = t_ans + timedelta(seconds=triage_s)
                encounter_start = t_tr
                doc = self._pick_physician(sl, lang, t_tr)
                events.append((ev("TRANSFERRED", t_tr, doc.login if doc else None), t_tr))
                t_end = self._create_encounter(d, t_tr, person, sl, "PHONE", "TEL", call_id, doc,
                                               wait_seconds=wait, weekend=weekend)
            else:
                t_end = t_ans + timedelta(seconds=triage_s)
            if r.random() < 0.005 * self.s:
                self._count("injected_calls_without_end")
            else:
                events.append((ev("ENDED", t_end), t_end))
        for e, ts in events:
            # events are pushed by the ACD within seconds; files are cut by delivery time
            self._emit_call(e, ts + timedelta(seconds=r.randint(1, 20)))
        if encounter_start and not any(e["event_type"] == "TRANSFERRED" for e, _ in events):
            doc = self._pick_physician(sl, lang, encounter_start)
            self._create_encounter(d, encounter_start, person, sl, "PHONE", "TEL", call_id, doc,
                                   wait_seconds=(encounter_start - t0).total_seconds(), weekend=weekend)
        self._count("calls")

    # ------------------------------------------------------------------ app
    def _app_session(self, d: date, t0: datetime, person: Person | None, sl: str, load: float, weekend: bool) -> None:
        r = self.r
        session_id = f"ses_{r.getrandbits(64):016x}"
        is_connect = sl == "PHARMACY_CONNECT"
        user = person.app_user_id if (person and person.app_user_id) else f"u_{r.getrandbits(48):012x}"
        if is_connect:
            platform = "pharmacy_terminal"
        else:
            platform = self._choice_w(["ios", "android", "web"], [42, 38, 20])
        day_idx = (d - self.start).days
        v5_share = 0 if day_idx < 45 else min(0.9, (day_idx - 45) / 20)
        version = "5.0.1" if r.random() < v5_share else f"4.8.{r.randint(0, 3)}"
        category = self._symptom_category(person, d) if person else "ADMIN"
        channel = "VIDEO" if is_connect else self._choice_w(["CHAT", "VIDEO"], [55, 45])
        skew = timedelta(0)
        if r.random() < 0.01 * self.s:                                     # device clock wrong
            skew = timedelta(hours=r.choice([-3, -2, 2, 3]), minutes=r.randint(0, 59))
            self._count("injected_app_clock_skew")

        def ev(name: str, ts: datetime, extra: dict | None = None) -> tuple[dict, datetime]:
            props = {"service_line": sl, "channel": channel, "symptom_category": category}
            if version.startswith("5"):
                props.update({"triage_score": r.randint(1, 10), "consent_version": "2026-08",
                              "wait_estimate_min": r.randint(1, 20)})
            else:
                props["wait_estimate_min"] = str(r.randint(1, 20))          # type drift: string in v4
            if extra:
                props.update(extra)
            server = ts + timedelta(milliseconds=r.randint(150, 3000))
            return ({"event_id": self._uuid(), "session_id": session_id, "app_user_id": user,
                     "event_name": name, "client_ts": int((ts + skew).timestamp() * 1000),
                     "server_ts": iso_z(server), "platform": platform, "app_version": version,
                     "properties": props}, server)

        events = [ev("app_open", t0)]
        t = t0 + timedelta(seconds=r.randint(5, 40))
        if not is_connect and r.random() < 0.5:
            events.append(ev("symptom_check_started", t))
            t += timedelta(seconds=r.randint(60, 300))
            events.append(ev("symptom_check_completed", t))
        encounter = False
        if is_connect or (person is not None and r.random() < 0.8):
            t += timedelta(seconds=r.randint(10, 60))
            events.append(ev("booking_created", t))
            if not is_connect and r.random() < 0.05:
                t += timedelta(seconds=r.randint(30, 600))
                events.append(ev("booking_cancelled", t))
            else:
                mean_wait = 180 * load ** 1.4
                wait = r.expovariate(1 / mean_wait)
                t_start = t + timedelta(seconds=wait)
                start_name = "video_started" if channel == "VIDEO" else "chat_started"
                events.append(ev(start_name, t_start))
                doc = self._pick_physician(sl, person.language, t_start)
                t_end = self._create_encounter(d, t_start, person, sl, channel, "APP", session_id, doc,
                                               wait_seconds=(t_start - t0).total_seconds(), weekend=weekend)
                events.append(ev("video_ended" if channel == "VIDEO" else "chat_ended", t_end))
                encounter = True
        for e, server in events:
            self._emit(self.w_app, server, e, 0.01, "injected_app_duplicates")
        if r.random() < 0.003 * self.s:                                    # QA / bot account noise
            bot = f"u_qa_{r.getrandbits(24):06x}"
            for _ in range(r.randint(20, 60)):
                ts = t0 + timedelta(seconds=r.randint(0, 60))
                self.w_app.add(ts, {"event_id": self._uuid(), "session_id": f"ses_qa_{r.getrandbits(32):08x}",
                                    "app_user_id": bot, "event_name": "app_open",
                                    "client_ts": int(ts.timestamp() * 1000), "server_ts": iso_z(ts),
                                    "platform": "web", "app_version": version,
                                    "properties": {"test_account": True}})
            self._count("injected_bot_sessions")
        self._count("app_sessions")
        if not encounter:
            self._count("app_sessions_without_encounter")

    # ------------------------------------------------------------------ staff selection
    def _pick_staff(self, team: str, lang: str, t: datetime) -> Staff | None:
        pool = [s for s in self.staff if s.team == team and s.active(t)]
        if not pool:
            return None
        speaking = [s for s in pool if lang in s.languages]
        return self.r.choice(speaking if speaking and self.r.random() < 0.9 else pool)

    def _pick_physician(self, sl: str, lang: str, t: datetime) -> Staff | None:
        team = {"KIDS_LINE": "PAEDIATRICS", "EMERGENCY": "EMERGENCY"}.get(sl, "GENERAL")
        return self._pick_staff(team, lang, t) or self._pick_staff("GENERAL", lang, t)

    def _symptom_category(self, person: Person, d: date) -> str:
        child = person.age_on(d) <= 15
        cats = list(ref.SYMPTOM_WEIGHTS)
        w = [ref.SYMPTOM_WEIGHTS[c][1 if child else 0] for c in cats]
        if person.sex == "F" and not child:
            w[cats.index("URINARY")] *= 2.5
        if person.age_on(d) >= 60:
            w[cats.index("CARDIO")] *= 3
        return self._choice_w(cats, w)

    # ------------------------------------------------------------------ EHR encounter
    def _create_encounter(self, d: date, start: datetime, person: Person, sl: str, channel: str,
                          contact_system: str, contact_ref: str, doc: Staff | None,
                          wait_seconds: float, weekend: bool) -> datetime:
        r = self.r
        if not person.registered:
            self._register(person, start - timedelta(seconds=r.randint(30, 120)))
        category = self._symptom_category(person, d)
        triage_w = ref.TRIAGE_WEIGHTS["EMERGENCY" if sl == "EMERGENCY" else "DEFAULT"]
        triage = self._choice_w([1, 2, 3, 4, 5], triage_w)
        if category == "CARDIO" and triage > 2 and r.random() < 0.4:
            triage -= 1
        disp_w = ref.DISPOSITION_WEIGHTS[triage]
        disposition = self._choice_w(list(disp_w), list(disp_w.values()))
        median = {"PHONE": 7, "VIDEO": 9, "CHAT": 14}[channel] * 60
        duration = max(90, r.lognormvariate(math.log(median), 0.45))
        end = start + timedelta(seconds=duration)
        cancelled = r.random() < 0.02
        if cancelled:
            end = start + timedelta(seconds=r.randint(20, 90))
        encounter_id = f"E{self._next('encounter'):09d}"
        local = start.astimezone(ZURICH)
        night = not (7 <= local.hour < 19)
        blocks = max(0, math.ceil((duration - 600) / 300))
        tariff = 0.0 if cancelled else (48 + 8 * blocks + (20 if night else 0) + (15 if weekend else 0)
                                        + (25 if sl == "EMERGENCY" else 0) + (5 if channel == "VIDEO" else 0))
        row = {"encounter_id": encounter_id, "patient_id": person.patient_id,
               "staff_id": doc.staff_id if doc else "", "channel": channel, "service_line": sl,
               "contact_system": contact_system, "contact_ref": contact_ref, "started_at": ehr_local(start),
               "ended_at": "", "triage_level": "", "disposition_code": "", "plan_code": person.plan_code,
               "tariff_amount_chf": "", "status": "OPEN", "is_deleted": 0,
               "created_at": ehr_local(start), "modified_at": ehr_local(start)}
        self.ehr["encounter"].write(row)                                     # v1: opened
        closed_at = end + timedelta(minutes=r.randint(1, 15))
        final = dict(row, ended_at=ehr_local(end), triage_level=triage,
                     disposition_code="" if cancelled else disposition,
                     tariff_amount_chf=f"{tariff:.2f}", status="CANCELLED" if cancelled else "CLOSED",
                     modified_at=ehr_local(closed_at))
        if r.random() < 0.002 * self.s:                                     # data issue: end before start
            final["ended_at"] = ehr_local(start - timedelta(minutes=r.randint(1, 30)))
            self._count("injected_end_before_start")
        if closed_at < self.horizon:
            self.ehr["encounter"].write(final)                              # v2: closed
        self._count("encounters")
        if r.random() < 0.003 * self.s:                                     # duplicate, later soft-deleted
            dup_id = f"E{self._next('encounter'):09d}"
            self.ehr["encounter"].write(dict(row, encounter_id=dup_id))
            deleted_at = start + timedelta(hours=r.randint(4, 30))
            if deleted_at < self.horizon:
                self.ehr["encounter"].write(dict(row, encounter_id=dup_id, is_deleted=1,
                                                 modified_at=ehr_local(deleted_at)))
            self._count("injected_duplicate_encounters")
        if cancelled or closed_at >= self.horizon:
            return end
        self._clinical_details(encounter_id, person, category, disposition, closed_at, end, d, final)
        return end

    def _clinical_details(self, encounter_id: str, person: Person, category: str, disposition: str,
                          closed_at: datetime, end: datetime, d: date, encounter_row: dict) -> None:
        r = self.r
        codes = [r.choice(self.icd_by_cat[category])]
        if r.random() < 0.3:
            extra = r.choice(ref.SECONDARY_SYMPTOM_CODES)
            if extra not in codes:
                codes.append(extra)
        if r.random() < 0.003 * self.s:
            codes[0] = r.choice(["J06.99", "X99.9", "J6.9"])                # invalid ICD-10 codes
            self._count("injected_invalid_icd")
        for i, code in enumerate(codes, start=1):
            self.ehr["encounter_diagnosis"].write({
                "encounter_id": encounter_id, "seq_no": i, "icd10_code": code,
                "diagnosis_role": "PRIMARY" if i == 1 else "SECONDARY", "is_deleted": 0,
                "created_at": ehr_local(closed_at), "modified_at": ehr_local(closed_at)})
        if r.random() < 0.08:                                               # late coding correction
            later = closed_at + timedelta(hours=r.randint(12, 72))
            if later < self.horizon:
                new_code = r.choice(self.icd_by_cat[category])
                self.ehr["encounter_diagnosis"].write({
                    "encounter_id": encounter_id, "seq_no": 1, "icd10_code": new_code,
                    "diagnosis_role": "PRIMARY", "is_deleted": 0,
                    "created_at": ehr_local(closed_at), "modified_at": ehr_local(later)})
                self.ehr["encounter"].write(dict(encounter_row, modified_at=ehr_local(later)))
                self._count("late_diagnosis_corrections")
        # prescriptions
        p_rx, meds = ref.PRESCRIBING[category]
        if r.random() < p_rx:
            for atc in r.sample(meds, k=min(len(meds), self._choice_w([1, 2], [70, 30]))):
                pharmacy = self._partner("PHARMACY", person.canton, d) if r.random() < 0.45 else None
                self.ehr["prescription"].write({
                    "prescription_id": f"RX{self._next('rx'):09d}", "encounter_id": encounter_id,
                    "atc_code": atc, "quantity": self._choice_w([1, 1, 1, 2, 3], [1, 1, 1, 1, 1]),
                    "pharmacy_partner_id": pharmacy.partner_id if pharmacy else "",
                    "issued_at": ehr_local(end), "is_deleted": 0,
                    "created_at": ehr_local(closed_at), "modified_at": ehr_local(closed_at)})
                self._count("prescriptions")
        # referral
        if disposition in ref.REFERRAL_TARGET and r.random() < 0.85:
            ptype, urgency = ref.REFERRAL_TARGET[disposition]
            partner = self._partner(ptype, person.canton, d)
            if partner:
                self.ehr["referral"].write({
                    "referral_id": f"RF{self._next('referral'):09d}", "encounter_id": encounter_id,
                    "partner_id": partner.partner_id, "referral_type": disposition, "urgency": urgency,
                    "issued_at": ehr_local(end), "is_deleted": 0,
                    "created_at": ehr_local(closed_at), "modified_at": ehr_local(closed_at)})
                self._count("referrals")
        # sick note
        age = person.age_on(d)
        if 18 <= age <= 65 and category != "ADMIN" and disposition in (
                "SELF_CARE", "PHARMACY", "GP_ROUTINE", "GP_24H") and r.random() < 0.18:
            self.ehr["sick_note"].write({
                "sick_note_id": f"SN{self._next('sick_note'):09d}", "encounter_id": encounter_id,
                "incapacity_pct": self._choice_w([100, 50], [90, 10]),
                "days": self._choice_w([1, 2, 3, 4, 5, 7], [20, 30, 25, 10, 10, 5]),
                "valid_from": end.astimezone(ZURICH).date().isoformat(), "issued_at": ehr_local(end),
                "is_deleted": 0, "created_at": ehr_local(closed_at), "modified_at": ehr_local(closed_at)})
            self._count("sick_notes")
        if encounter_row["service_line"] == "PHARMACY_CONNECT":
            self._measurements(encounter_id, person, end, d)

    def _partner(self, ptype: str, canton: str, d: date) -> Partner | None:
        if not hasattr(self, "_pt_idx"):
            self._pt_idx: dict[tuple, list[Partner]] = {}
            for p in self.partners:
                self._pt_idx.setdefault((p.partner_type, p.canton), []).append(p)
                self._pt_idx.setdefault((p.partner_type, "*"), []).append(p)
        pool = [p for p in self._pt_idx.get((ptype, canton), []) if p.active]
        if not pool:
            pool = [p for p in self._pt_idx.get((ptype, "*"), []) if p.active]
        return self.r.choice(pool) if pool else None

    # ------------------------------------------------------------------ device measurements
    def _measurements(self, encounter_id: str, person: Person, end: datetime, d: date) -> None:
        r = self.r
        devices = [x for x in self.devices if x.status == "ACTIVE" and x.installed_on <= d]
        local = [x for x in devices if self._partner_by_id(x.partner_id).canton == person.canton]
        dev = r.choice(local or devices)
        temp_unit, glu_unit = ref.DEVICE_MODELS[dev.model]
        metrics = ["SBP", "DBP", "HR"] + (["SPO2"] if r.random() < 0.8 else []) + \
                  (["TEMP"] if r.random() < 0.7 else []) + (["GLU"] if r.random() < 0.25 else [])
        sick = r.random() < 0.15
        t = end - timedelta(minutes=r.randint(3, 8))
        for m in metrics:
            mean, sd = ref.VITAL_BASELINES[m]
            v = r.gauss(mean, sd)
            if sick:
                v += {"SBP": 35, "DBP": 15, "HR": 30, "SPO2": -6, "TEMP": 1.6, "GLU": 90}[m]
            if m == "SPO2":
                v = min(v, 100)
            unit = {"SBP": "mmHg", "DBP": "mmHg", "HR": "bpm", "SPO2": "%", "TEMP": temp_unit, "GLU": glu_unit}[m]
            if unit == "degF":
                v = v * 9 / 5 + 32
            if unit == "mmol/L":
                v = v / 18.0182
            if r.random() < 0.005 * self.s:
                v = r.choice([0.0, 999.0, -1.0])
                self._count("injected_device_glitch")
            received = t + timedelta(seconds=r.randint(5, 120))
            if r.random() < 0.02 * self.s:
                received += timedelta(hours=r.randint(3, 20))                # offline sync
                self._count("injected_device_late")
            rec = {"measurement_id": self._uuid(), "device_serial": dev.serial, "partner_id": dev.partner_id,
                   "encounter_ref": None if r.random() < 0.01 * self.s else encounter_id,
                   "metric": m, "value": round(v, 2), "unit": unit, "measured_at": iso_z(t, millis=False),
                   "received_at": iso_z(received), "firmware_version": dev.firmware}
            self._emit(self.w_meas, min(received, self.horizon - timedelta(seconds=1)), rec, 0.02,
                       "injected_device_duplicates")
            self._count("measurements")
            t += timedelta(seconds=r.randint(20, 60))

    def _partner_by_id(self, pid: str) -> Partner:
        if not hasattr(self, "_pidx"):
            self._pidx = {p.partner_id: p for p in self.partners}
        return self._pidx[pid]

    # ------------------------------------------------------------------ summary
    def summary(self) -> dict:
        out = dict(sorted(self.stats.items()))
        out.update({
            "call_event_rows": self.w_calls.rows, "app_event_rows": self.w_app.rows,
            "measurement_rows": self.w_meas.rows,
            "files_call_events": len(self.w_calls.files), "files_app_events": len(self.w_app.files),
            "files_measurements": len(self.w_meas.files),
            **{f"ehr_{t}_rows": w.rows for t, w in self.ehr.items()},
            "people_total": len(self.people), "staff": len(self.staff), "partners": len(self.partners),
            "devices": len(self.devices), "coverage_records": len(self.coverage),
        })
        return out
