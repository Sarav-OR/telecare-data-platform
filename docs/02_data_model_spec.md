# TeleCare CH – Data model specification (v1.0)

TeleCare CH is a **fictional** Swiss telemedicine provider used to demonstrate a production-style
Azure Data Factory + Databricks + dbt Cloud platform, including a **Data Vault → Kimball**
migration. The business model is modelled on public descriptions of Swiss telemedicine
operations: a 24/7 medical centre reached by phone, video, chat and app; triage; e-prescriptions,
referrals and sick notes; Telmed insurance models; a partner network of pharmacies and practices;
and pharmacy-based diagnostics. **All data is synthetic.** Code lists use public standards
(ICD-10-GM, ATC).

---

## 1. Business processes in scope

| # | Process | Question it answers |
|---|---|---|
| P1 | **Patient contact** – a patient reaches TeleCare by phone or app | How fast do we answer? How many give up? Which channels and lines are busy when? |
| P2 | **Teleconsultation (encounter)** – a clinician triages and treats | What do patients call about? How many cases are resolved remotely? What is prescribed, referred, certified? |
| P3 | **Pharmacy diagnostics** – measurements taken in a partner pharmacy during an encounter | Which vitals are measured, how often are values critical, per pharmacy? |

"Encounter" follows the HL7 FHIR meaning: one interaction between a patient and a clinician
(here a *virtual* encounter), including triage and its outcome.

---

## 2. Source systems (5) and feeds (16)

| # | Source system | Feed | Format / ADF connector | Frequency | Load pattern |
|---|---|---|---|---|---|
| S1 | **Contact-centre telephony** | `call_events` | JSON Lines files → ADLS | hourly | append-only events |
| S2 | **Patient app & web** | `app_events` | JSON Lines (nested `properties`) → ADLS | hourly | append-only events |
| S3 | **Clinical system (EHR)** | `patient`, `patient_identifier`, `staff`, `encounter`, `encounter_diagnosis`, `prescription`, `referral`, `sick_note` | **Azure SQL Database** → ADF copy with `modified_at` watermark | daily (incremental) | upserts + soft deletes |
| S4 | **Partner & insurer management (CRM)** | `insurer`, `insurance_plan`, `patient_coverage` | CSV full snapshots (`;`, dd.mm.yyyy) → ADLS | weekly | snapshot → change detection |
| S4 | *(same system)* | `partner_directory` | **REST API**, paginated JSON → ADLS | weekly | full refresh |
| S5 | **Pharmacy diagnostics devices** | `device_measurements` (+ weekly `device_registry` CSV) | JSON Lines device telemetry → ADLS | hourly | append-only events |

Plus **reference data** owned by the business, versioned in Git as dbt seeds: ICD-10-GM subset,
ATC subset, triage levels, dispositions, service lines, channels, queues, contact outcomes,
tariff periods, clinical metric thresholds, cantons.

### 2.1 Why these are heterogeneous
Four connector types (files, database, REST API, CSV), three latencies (hourly, daily, weekly),
three load patterns (events, incremental upserts, snapshots) and **three different patient
identifiers**:

| System | Patient identifier |
|---|---|
| Telephony | `caller_phone_hash` (salted SHA-256 of the number, never the number) |
| App | `app_user_id` |
| EHR | `patient_id` – the master; `patient_identifier` holds verified phone hashes and app user IDs |

Resolving them to one patient is modelled as a Data Vault **same-as link** and exposed in Kimball
as `map_patient_identity`.

### 2.2 Source feed details

**S1 `call_events`** – one row per telephony event
`event_id, call_id, sequence_no, event_type (OFFERED | QUEUED | ANSWERED | TRANSFERRED | CALLBACK_REQUESTED | ABANDONED | ENDED), event_ts, dialled_line, queue_code, agent_login, caller_phone_hash, ivr_language`

**S2 `app_events`** – one row per app event
`event_id, session_id, app_user_id, event_name (app_open | symptom_check_started | symptom_check_completed | booking_created | booking_cancelled | chat_started | chat_ended | video_started | video_ended), client_ts, server_ts, platform, app_version, properties{service_line, channel, symptom_category, encounter_ref, …}`

**S3 EHR tables** (all with `created_at`, `modified_at`, `is_deleted`)

| Table | Key columns |
|---|---|
| `patient` | `patient_id, birth_year, sex, canton, postal_code, preferred_language` (no names or dates of birth – privacy by design) |
| `patient_identifier` | `patient_id, identifier_type (PHONE_HASH / APP_USER_ID), identifier_value, verified_at` |
| `staff` | `staff_id, staff_login, role (PHYSICIAN / MEDICAL_ASSISTANT), specialty, team (GENERAL / PAEDIATRICS / EMERGENCY), languages, employment_pct` |
| `encounter` | `encounter_id, patient_id, staff_id, channel, service_line, contact_ref (call_id or session_id), started_at, ended_at, triage_level, disposition_code, plan_code, tariff_amount_chf, status (OPEN / CLOSED / CANCELLED)` |
| `encounter_diagnosis` | `encounter_id, seq_no, icd10_code, diagnosis_role (PRIMARY / SECONDARY)` |
| `prescription` | `prescription_id, encounter_id, atc_code, quantity, pharmacy_partner_id, issued_at` |
| `referral` | `referral_id, encounter_id, partner_id, referral_type (GP / SPECIALIST / EMERGENCY / PHARMACY_CONNECT), urgency, issued_at` |
| `sick_note` | `sick_note_id, encounter_id, incapacity_pct, days, issued_at` |

**S4 CRM**: `insurer(insurer_id, insurer_name, …)`, `insurance_plan(plan_code, insurer_id, model (TELMED / HMO / FAMILY_DOCTOR / STANDARD), telmed_first_contact_required)`, `patient_coverage(patient_id, plan_code, valid_from, valid_to)`, `partner_directory(partner_id, partner_type (PHARMACY / GP_PRACTICE / SPECIALIST / HOSPITAL_ED), name, canton, city, postal_code, connect_enabled, active)`

**S5 devices**: `measurement_id, device_serial, partner_id, encounter_ref, metric, value, unit, measured_at, received_at` and `device_registry(device_serial, model, manufacturer, firmware_version, partner_id, status)`

### 2.3 Injected data-quality problems (what the pipeline must catch)

| Source | Problems |
|---|---|
| S1 telephony | duplicate events, out-of-order events, calls without `ENDED`, unknown queue codes, late files |
| S2 app | client clock skew (`client_ts` in the future), new `properties` fields after an app release (schema drift), test/bot users, duplicates |
| S3 EHR | encounters updated after closing (late diagnosis coding), soft deletes, invalid ICD-10 codes, `ended_at < started_at` |
| S4 CRM | overlapping coverage periods, plan changes (SCD2), duplicate snapshot rows, partner deactivations |
| S5 devices | impossible values, unit variants (°F, mmol/L), missing `encounter_ref`, duplicates, late sync |

---

## 3. Layered architecture and schemas (Unity Catalog `telecare_dev` / `telecare_prd`)

| Schema | Built by | Content |
|---|---|---|
| `bronze` | Databricks (Auto Loader) | raw copy of each feed, all strings, lineage columns |
| `silver` | Databricks | typed, validated, de-duplicated; DQ errors quarantined |
| `ops` | Databricks | DQ rule results, quarantine, batch run log |
| `reference` | dbt seeds | code lists and thresholds |
| `staging` | dbt (views) | hash keys, hashdiffs, record source |
| `raw_vault` | dbt (incremental) | hubs, links, satellites – full history, insert-only |
| `business_vault` | dbt | derived business rules (call sessionisation, identity resolution) |
| `marts` | dbt | Kimball star schemas for BI |

---

## 4. Raw vault (integration and history layer) - as built

**Hubs (12)**: `hub_patient` (business-key collision code EHR / TEL / APP), `hub_staff`, `hub_encounter`,
`hub_call`, `hub_app_session`, `hub_prescription`, `hub_referral`, `hub_sick_note`, `hub_partner`,
`hub_insurer`, `hub_insurance_plan`, `hub_device`

**Links (9)**: `link_encounter` (encounter-patient-staff), `link_encounter_call`, `link_encounter_app_session`,
`link_prescription` (prescription-encounter-pharmacy), `link_referral` (referral-encounter-partner),
`link_sick_note`, `link_patient_plan`, `link_plan_insurer`, `sal_patient` (**same-as link**: TEL / APP identity ↔ EHR patient)

**Non-historised links (3)**: `nhl_call_event`, `nhl_app_event`, `nhl_device_measurement`

**Satellites (13)**: `sat_patient_ehr` (PII column masked), `sat_patient_identifier` (on the same-as link),
`sat_staff`, `sat_encounter`, `msat_encounter_diagnosis` (multi-active), `sat_prescription`, `sat_referral`,
`sat_sick_note`, `sat_partner`, `sat_insurer`, `sat_insurance_plan`, `effsat_patient_coverage` (effectivity), `sat_device`

**Business vault (3)**: `bv_call_session` (events → one row per call: queue time, service level, outcome),
`bv_app_session` (test accounts removed), `bv_patient_identity` (identity resolution, shared phones flagged)

All vault loads are insert-only and idempotent (re-running a load adds nothing).

---

## 5. Kimball marts (consumption layer)

### 5.1 Facts

| Fact | Type | Grain | Key measures |
|---|---|---|---|
| **`fact_encounter`** | accumulating snapshot (milestones updated in place) | one row per encounter | wait seconds, consultation seconds, tariff CHF, triage level, flags: resolved remotely, referred, prescribed, sick note, emergency escalation; counts of diagnoses / prescriptions |
| **`fact_contact`** | transaction | one row per inbound contact (call or app session) | queue seconds, handle seconds, answered, answered within service level (≤ 60 s), abandoned, callback requested, converted to encounter |
| **`fact_diagnostic_measurement`** | transaction | one row per device measurement | value (standard unit), alert level (NORMAL / WARNING / CRITICAL), latency seconds |

### 5.2 Dimensions (16) – ★ = SCD2

| Dimension | Source | Notes |
|---|---|---|
| `dim_date` | generated | calendar, ISO week, Swiss public holidays flag |
| `dim_time_of_day` | generated | minute grain, daypart, night-tariff flag |
| ★ `dim_patient` | EHR via vault | age band (not birth year), sex, canton, language; unknown member for unidentified callers |
| ★ `dim_staff` | EHR | role, specialty, team |
| ★ `dim_insurance_plan` | CRM | plan, insurer, model (Telmed …) |
| ★ `dim_partner` | CRM API | type, canton, Connect-enabled |
| ★ `dim_device` | device registry | model, firmware |
| `dim_channel` | seed | phone, video, chat, app |
| `dim_service_line` | seed | 24/7 centre, emergency line, kids line, pharmacy Connect |
| `dim_triage_level` | seed | 1 (immediate) … 5 (non-urgent) |
| `dim_disposition` | seed | self-care, pharmacy, GP within 24 h, specialist, emergency department |
| `dim_diagnosis` | seed (ICD-10-GM) | code, block, chapter |
| `dim_medication` | seed (ATC) | ATC levels 1–5 |
| `dim_queue` | seed | queue, skill, language |
| `dim_contact_outcome` | seed | answered, abandoned, callback, transferred |
| `dim_metric` | seed | vital sign, unit, clinical thresholds |

### 5.3 Bridges and mapping (3)

| Table | Purpose |
|---|---|
| `bridge_encounter_diagnosis` | many diagnoses per encounter; carries `diagnosis_role` and a weighting factor so totals are not double-counted |
| `bridge_encounter_medication` | many prescribed medications per encounter |
| `map_patient_identity` | phone hash / app user ID → patient key (for contact-to-encounter analysis) |

### 5.4 Bus matrix (conformed dimensions)

| Dimension → / Fact ↓ | date | time | patient | staff | channel | service line | plan | partner | triage | disposition | diagnosis* | medication* | queue | contact outcome | device | metric |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `fact_encounter` | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ referral | ✔ | ✔ | ✔ bridge | ✔ bridge | | | | |
| `fact_contact` | ✔ | ✔ | ✔ | ✔ agent | ✔ | ✔ | | | | | | | ✔ | ✔ | | |
| `fact_diagnostic_measurement` | ✔ | ✔ | ✔ | | | ✔ | | ✔ pharmacy | | | | | | | ✔ | ✔ |

The six dimensions shared across facts (date, time, patient, staff, channel, service line) are
**conformed**: one definition, used by every fact, so contact → encounter → measurement can be
analysed together.

### 5.5 Key business rules (implemented in dbt, tested)

| Rule | Where |
|---|---|
| Service level = answered within 60 s of `QUEUED` | `bv_call_session` → `fact_contact` |
| Abandoned = `ABANDONED` before `ANSWERED`; calls without `ENDED` after 4 h are closed as `TIMEOUT` | `bv_call_session` |
| Resolved remotely = disposition in (self-care, pharmacy) and no referral | `fact_encounter` |
| Night tariff = 19:00–07:00 or weekend / public holiday | `dim_time_of_day` + `dim_date` |
| Patient attributes as of encounter start (point-in-time SCD2 lookup) | all facts |
| Unknown codes (ICD, ATC, queue) → unknown member, never dropped | all facts |

---

## 6. Table count

| Layer | Tables |
|---|---|
| Bronze | 16 |
| Silver | 16 |
| Ops | 3 |
| Reference (seeds) | 12 |
| Staging (views) | 16 |
| Raw vault | 37 (12 hubs, 9 links, 3 NHL, 13 sats) |
| Business vault | 3 |
| **Kimball marts** | **22** (16 dims, 2 bridges, 1 map, 3 facts) |
| **Total** | **~125 objects** |

---

## 7. Data volume (default generator settings)

| Setting | Value |
|---|---|
| Period | 90 days (2026-06-25 → 2026-09-22) |
| Patients | 20,000 (about 8,000 active in the period) |
| Staff | ~160 physicians, ~120 medical assistants |
| Partners / insurers / plans | ~1,500 / 10 / ~40 |

| Entity (generated, default seed) | ≈ Rows |
|---|---|
| Inbound contacts | 207,000 (146,000 calls + 61,000 app sessions) |
| Telephony events | 673,000 |
| App events | 274,000 |
| Encounters | 147,000 (306,000 row versions in the EHR) |
| Diagnoses | 197,000 row versions |
| Prescriptions / referrals / sick notes | 80,000 / 61,000 / 13,000 |
| Device measurements | 16,000 |
| CRM coverage snapshot rows (weekly) | 347,000 |
| **Total** | **≈ 2 million source rows (~470 MB raw)** |

Realistic patterns: Monday and evening peaks, more emergency-line calls at night, kids line for
patients under 16, and the Telmed plan share driving first-contact volume.

---

## 8. Data quality: two levels

| Level | Tool | Examples | Action |
|---|---|---|---|
| Technical / plausibility | Databricks DQ engine (YAML rules) | malformed JSON, missing keys, impossible values, future timestamps | quarantine + metrics + circuit breaker |
| Business / integrity | dbt tests | referential integrity, SCD2 without overlaps, service-level logic, vault-vs-Kimball reconciliation | fail the dbt job; failures stored for triage |

---

## 9. Migration: Data Vault → Kimball

- The **raw vault stays** as the auditable, source-aligned history layer.
- **Business-facing consumption moves from vault views to Kimball stars**, built on the vault.
- **Reconciliation tests prove nothing is lost:**
  - encounter count in `fact_encounter` = `hub_encounter` (minus soft-deleted)
  - SCD2 versions in `dim_patient` = `sat_patient_ehr` changes
  - diagnosis rows in the bridge = active rows in `msat_encounter_diagnosis`
  - tariff totals equal per month
- The documented rollout is parallel run → reconcile → switch BI → decommission the old vault
  information marts.

---

## 10. Privacy by design
- No names, addresses or dates of birth anywhere. Only a pseudonymous `patient_id`, birth year
  (shown as an age band in marts) and canton.
- Phone numbers are salted-hashed **at the source**; the platform never sees a number.
- Diagnoses are joined to patients only in marts, which have restricted access (Unity Catalog grants).

---

## 11. Decisions taken
1. Volume: 90 days × 20,000 patients (confirmed).
2. No 15-minute periodic snapshot fact (adds a pattern, not a point).
3. PII: date of birth stored in source, bronze and silver (restricted zone: schema access for pipeline
   identities and data engineers only, column tagged `pii`). The Unity Catalog column mask
   (`pii_readers` group) sits on the consumer-facing raw vault satellite `sat_patient_ehr`, because
   dedicated job clusters cannot read or MERGE into masked tables. Marts expose only
   `age_at_encounter` and `age_band`.
4. CRM and device-registry snapshots are weekly.
