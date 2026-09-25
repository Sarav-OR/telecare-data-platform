"""Reference data and clinical knowledge used by the generator.

Code lists (ICD-10, ATC, queues, ...) are read from the dbt seeds so there is a
single source of truth: every valid code the generator emits exists in the
reference tables the marts join to.
"""

from __future__ import annotations

import csv
from pathlib import Path

SEEDS_DIR = Path(__file__).resolve().parents[2] / "dbt" / "seeds"


def load_seed(name: str) -> list[dict]:
    with (SEEDS_DIR / f"{name}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# postal code ranges per canton (approximate, for realistic-looking data only)
POSTAL_RANGES = {
    "ZH": (8000, 8999), "BE": (3000, 3999), "LU": (6000, 6299), "UR": (6460, 6499),
    "SZ": (6400, 6459), "OW": (6060, 6078), "NW": (6362, 6390), "GL": (8750, 8784),
    "ZG": (6300, 6345), "FR": (1700, 1799), "SO": (4500, 4599), "BS": (4000, 4059),
    "BL": (4100, 4499), "SH": (8200, 8262), "AR": (9100, 9112), "AI": (9050, 9058),
    "SG": (9000, 9499), "GR": (7000, 7799), "AG": (5000, 5799), "TG": (8500, 8599),
    "TI": (6500, 6999), "VD": (1000, 1499), "VS": (1870, 1999), "NE": (2000, 2499),
    "GE": (1200, 1299), "JU": (2800, 2999),
}
EXPAT_CANTONS = {"ZH", "GE", "ZG", "BS", "VD"}

# symptom category -> (weight adult, weight child <= 15)
SYMPTOM_WEIGHTS = {
    "RESPIRATORY": (24, 26), "GASTRO": (13, 16), "ENT": (7, 16), "URINARY": (7, 1),
    "SKIN_ALLERGY": (8, 8), "MUSCULOSKELETAL": (10, 1), "INJURY": (8, 9), "CARDIO": (4, 0.2),
    "NEURO": (7, 3), "MENTAL": (3, 0.5), "EYE": (3, 4), "GENERAL": (6, 15), "ADMIN": (3, 0.3),
}

# symptom category -> candidate medications (ATC) and probability that anything is prescribed
PRESCRIBING = {
    "RESPIRATORY": (0.40, ["N02BE01", "R05CB01", "R01AA07", "J01CA04", "J01CR02", "J01FA10", "R03AC02"]),
    "GASTRO": (0.35, ["A07DA03", "A03FA03", "A02BC01", "A02BC02", "A06AD15"]),
    "ENT": (0.45, ["N02BE01", "M01AE01", "J01CA04", "R01AA07"]),
    "URINARY": (0.85, ["J01XX01", "J01XE01", "J01MA02"]),
    "SKIN_ALLERGY": (0.50, ["R06AE07", "R06AX27", "D07AA02", "H02AB06"]),
    "MUSCULOSKELETAL": (0.45, ["M01AE01", "M01AE02", "M01AB05", "N02BE01"]),
    "INJURY": (0.30, ["M01AE01", "N02BE01", "M01AB05"]),
    "CARDIO": (0.10, ["N02BE01"]),
    "NEURO": (0.35, ["N02BE01", "M01AE01", "N02CC01"]),
    "MENTAL": (0.15, ["N05BA01"]),
    "EYE": (0.60, ["S01AA01", "S01GX09"]),
    "GENERAL": (0.30, ["N02BE01", "M01AE01"]),
    "ADMIN": (0.95, ["A02BC02", "R06AE07", "N02CC01", "R03AC02"]),
}

# generic symptom codes that are often added as a secondary diagnosis
SECONDARY_SYMPTOM_CODES = ["R50.9", "R05", "R51", "R53", "R10.4"]

# triage level distribution per service line
TRIAGE_WEIGHTS = {
    "EMERGENCY": [8, 25, 40, 20, 7],
    "DEFAULT": [0.5, 4, 20, 42, 33.5],
}

# disposition distribution per triage level
DISPOSITION_WEIGHTS = {
    1: {"AMBULANCE": 60, "EMERGENCY_DEPT": 40},
    2: {"EMERGENCY_DEPT": 60, "GP_24H": 40},
    3: {"GP_24H": 45, "SPECIALIST": 10, "PHARMACY": 15, "SELF_CARE": 30},
    4: {"SELF_CARE": 50, "PHARMACY": 20, "GP_ROUTINE": 20, "SPECIALIST": 10},
    5: {"SELF_CARE": 75, "PHARMACY": 15, "GP_ROUTINE": 10},
}

# disposition -> partner type of the referral target
REFERRAL_TARGET = {
    "GP_ROUTINE": ("GP_PRACTICE", "ROUTINE"), "GP_24H": ("GP_PRACTICE", "WITHIN_24H"),
    "SPECIALIST": ("SPECIALIST", "ROUTINE"), "EMERGENCY_DEPT": ("HOSPITAL_ED", "URGENT"),
    "AMBULANCE": ("HOSPITAL_ED", "EMERGENCY"), "PHARMACY": ("PHARMACY", "ROUTINE"),
}

# relative contact volume by hour of day (UTC+2 local hours are applied by the generator)
HOUR_WEIGHTS_WEEKDAY = [1.2, 0.8, 0.6, 0.5, 0.5, 0.7, 1.5, 3.5, 6.0, 6.5, 6.0, 5.5,
                        5.0, 4.8, 4.8, 4.8, 5.0, 5.5, 6.0, 6.0, 5.0, 3.8, 2.6, 1.8]
HOUR_WEIGHTS_WEEKEND = [1.6, 1.1, 0.8, 0.6, 0.6, 0.8, 1.4, 2.8, 4.8, 5.6, 5.8, 5.6,
                        5.2, 5.0, 5.0, 5.0, 5.2, 5.4, 5.6, 5.4, 4.6, 3.8, 2.8, 2.0]
WEEKDAY_FACTOR = [1.25, 1.02, 0.98, 0.97, 0.95, 1.08, 1.12]   # Mon..Sun

INSURERS = [
    ("INS01", "AlpenVita Kranken"), ("INS02", "RheinCare Versicherung"), ("INS03", "JuraSanté Mutuelle"),
    ("INS04", "TicinoSalus"), ("INS05", "BodenseeKasse"), ("INS06", "AareMed"),
    ("INS07", "LémanSanté"), ("INS08", "SäntisVita"), ("INS09", "Gotthard Health"),
    ("INS10", "Engadina Assicura"),
]
PLAN_MODELS = [("TEL", "TELMED", 45, True, 18), ("HMO", "HMO", 15, False, 20),
               ("FAM", "FAMILY_DOCTOR", 25, False, 12), ("STD", "STANDARD", 15, False, 0)]

CANTON_CAPITALS = {
    "ZH": "Zürich", "BE": "Bern", "LU": "Luzern", "UR": "Altdorf", "SZ": "Schwyz", "OW": "Sarnen",
    "NW": "Stans", "GL": "Glarus", "ZG": "Zug", "FR": "Fribourg", "SO": "Solothurn", "BS": "Basel",
    "BL": "Liestal", "SH": "Schaffhausen", "AR": "Herisau", "AI": "Appenzell", "SG": "St. Gallen",
    "GR": "Chur", "AG": "Aarau", "TG": "Frauenfeld", "TI": "Bellinzona", "VD": "Lausanne",
    "VS": "Sion", "NE": "Neuchâtel", "GE": "Genève", "JU": "Delémont",
}
PARTNER_NAME_PARTS = {
    "de": {"PHARMACY": "Apotheke", "GP_PRACTICE": "Hausarztpraxis", "SPECIALIST": "Facharztzentrum",
           "HOSPITAL_ED": "Spital Notfall"},
    "fr": {"PHARMACY": "Pharmacie", "GP_PRACTICE": "Cabinet médical", "SPECIALIST": "Centre spécialisé",
           "HOSPITAL_ED": "Urgences Hôpital"},
    "it": {"PHARMACY": "Farmacia", "GP_PRACTICE": "Studio medico", "SPECIALIST": "Centro specialistico",
           "HOSPITAL_ED": "Pronto soccorso Ospedale"},
}
PARTNER_SUFFIXES = ["Zentrum", "Bahnhof", "am See", "Altstadt", "Nord", "Süd", "Ost", "West",
                    "Central", "du Lac", "Plaza", "Parc", "Stazione", "Markt", "Brücke", "Rosengarten"]

DEVICE_MODELS = {   # model -> (temperature unit, glucose unit)
    "VitalCheck Station 2": ("degF", "mmol/L"),
    "VitalCheck Station 3": ("degC", "mg/dL"),
}

# baseline (mean, sd) for pharmacy vital-sign measurements in standard units
VITAL_BASELINES = {"SBP": (128, 16), "DBP": (81, 10), "HR": (78, 12), "SPO2": (97, 1.5),
                   "TEMP": (37.0, 0.6), "GLU": (112, 28)}
