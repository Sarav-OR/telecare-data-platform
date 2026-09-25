# Expected counts after the Day 2 bootstrap run (landing up to 2026-09-15)

Produced by the identical local run (`run_local_pipeline.py --until 2026-09-15`, seed 42).
Databricks should match exactly for silver; bronze can be higher if files were uploaded twice.

| feed | bronze rows in | rejected (quarantine) | silver rows |
|---|---:|---:|---:|
| `call_events` | 619,188 | 194 (0.03%) | 612,832 |
| `app_events` | 253,911 | 78 (0.03%) | 251,374 |
| `device_measurements` | 15,061 | 79 (0.52%) | 14,668 |
| `device_registry` | 2,559 | 0 (0.00%) | 2,559 |
| `crm_insurers` | 130 | 0 (0.00%) | 130 |
| `crm_insurance_plans` | 520 | 0 (0.00%) | 520 |
| `crm_patient_coverage` | 317,466 | 0 (0.00%) | 317,453 |
| `crm_partners` | 19,500 | 0 (0.00%) | 19,500 |
| `ehr_patient` | 29,232 | 0 (0.00%) | 29,232 |
| `ehr_patient_identifier` | 40,804 | 0 (0.00%) | 40,804 |
| `ehr_staff` | 296 | 0 (0.00%) | 296 |
| `ehr_encounter` | 146,893 | 274 (0.19%) | 146,619 |
| `ehr_encounter_diagnosis` | 181,555 | 134 (0.07%) | 181,421 |
| `ehr_prescription` | 73,679 | 0 (0.00%) | 73,679 |
| `ehr_referral` | 56,442 | 0 (0.00%) | 56,442 |
| `ehr_sick_note` | 12,115 | 0 (0.00%) | 12,115 |
