# Day 5 – Power BI report on the Kimball marts

Goal: a 3-page report that answers real telemedicine questions from the **production marts**
(`telecare_dev.marts`), built as a clean star schema. ~60–90 min.

**Why Import mode (not DirectQuery):** the marts are small (≈150k encounters), the report is refreshed
after the daily load, and Import makes the report fast and keeps the SQL warehouse stopped between
refreshes (cost). DirectQuery would wake the warehouse on every click.

---

## P1. Connect (10 min)

Power BI Desktop → **Get data → Azure Databricks**:

| Field | Value |
|---|---|
| Server hostname | `adb-7405609552599914.14.azuredatabricks.net` |
| HTTP path | `/sql/1.0/warehouses/58529f7ffc07a8a6` |
| Catalog / Database | `telecare_dev` / leave empty |
| Data connectivity mode | **Import** |

Authentication: **Microsoft Entra ID** → sign in (no token needed). The warehouse starts (~1 min).

In the Navigator, open `telecare_dev → marts` and tick:

| Facts | Dimensions | Bridge |
|---|---|---|
| `fact_encounter` | `dim_date`, `dim_time_of_day`, `dim_patient`, `dim_service_line`, `dim_channel`, `dim_triage_level`, `dim_disposition`, `dim_queue`, `dim_contact_outcome`, `dim_diagnosis` | `bridge_encounter_diagnosis` |
| `fact_contact` | | |

→ **Load**. (Power BI reads the data **as you**: you are in `pii_readers`, but the marts contain no
date of birth anyway – only ages.)

## P2. Model – relationships (15 min)

**Model view.** Delete any relationships Power BI guessed, then create these (all *single* direction,
many-to-one, from fact to dimension):

| From (many) | To (one) |
|---|---|
| `fact_encounter[date_key]` | `dim_date[date_key]` |
| `fact_encounter[time_key]` | `dim_time_of_day[time_key]` |
| `fact_encounter[patient_sk]` | `dim_patient[patient_sk]` |
| `fact_encounter[service_line_key]` | `dim_service_line[service_line_key]` |
| `fact_encounter[channel_key]` | `dim_channel[channel_key]` |
| `fact_encounter[triage_level_key]` | `dim_triage_level[triage_level_key]` |
| `fact_encounter[disposition_key]` | `dim_disposition[disposition_key]` |
| `fact_contact[date_key]` | `dim_date[date_key]` |
| `fact_contact[time_key]` | `dim_time_of_day[time_key]` |
| `fact_contact[queue_key]` | `dim_queue[queue_key]` |
| `fact_contact[contact_outcome_key]` | `dim_contact_outcome[contact_outcome_key]` |
| `fact_contact[channel_key]` | `dim_channel[channel_key]` |
| `fact_contact[service_line_key]` | `dim_service_line[service_line_key]` |
| `bridge_encounter_diagnosis[encounter_key]` | `fact_encounter[encounter_key]` – **both directions** (bridge pattern) |
| `bridge_encounter_diagnosis[diagnosis_key]` | `dim_diagnosis[diagnosis_key]` |

Mark `dim_date` as date table: select it → **Table tools → Mark as date table** → `calendar_date`.

**Design note:** conformed dimensions (`dim_date`, `dim_channel`, `dim_service_line`) are shared by
both facts, so one slicer filters encounters and contacts consistently. The diagnosis bridge
resolves the many-to-many between encounters and diagnoses; `weighting_factor` avoids double counting.

## P3. Measures (10 min)

Create a table `_Measures` (Home → Enter data → empty table named `_Measures`) and add:

```DAX
Encounters = CALCULATE ( COUNTROWS ( fact_encounter ), fact_encounter[is_deleted] = FALSE () )

Remote Resolution % =
DIVIDE ( CALCULATE ( [Encounters], fact_encounter[is_resolved_remotely] = TRUE () ), [Encounters] )

Avg Consultation (min) =
DIVIDE ( CALCULATE ( SUM ( fact_encounter[consultation_seconds] ), fact_encounter[is_deleted] = FALSE () ), [Encounters] ) / 60

Avg Wait (min) =
DIVIDE ( CALCULATE ( SUM ( fact_encounter[wait_seconds] ), fact_encounter[is_deleted] = FALSE () ), [Encounters] ) / 60

Emergency Escalation % =
DIVIDE ( CALCULATE ( [Encounters], fact_encounter[is_emergency_escalation] = TRUE () ), [Encounters] )

Prescription Rate % =
DIVIDE ( CALCULATE ( [Encounters], fact_encounter[has_prescription] = TRUE () ), [Encounters] )

Contacts = COUNTROWS ( fact_contact )

Service Level % =
DIVIDE ( CALCULATE ( [Contacts], fact_contact[is_answered_within_sl] = TRUE () ),
         CALCULATE ( [Contacts], fact_contact[contact_system] = "TEL" ) )

Abandon Rate % =
DIVIDE ( CALCULATE ( [Contacts], fact_contact[is_abandoned] = TRUE () ),
         CALCULATE ( [Contacts], fact_contact[contact_system] = "TEL" ) )

Contact → Encounter % =
DIVIDE ( CALCULATE ( [Contacts], fact_contact[is_converted_to_encounter] = TRUE () ), [Contacts] )

Weighted Diagnoses = SUM ( bridge_encounter_diagnosis[weighting_factor] )
```

Format the `%` measures as percentage (1 decimal).

## P4. Report pages (30 min)

**Page 1 – Operations (contact centre)**
- Cards: Contacts · Service Level % · Abandon Rate % · Contact → Encounter %
- Line chart: Contacts by `dim_date[calendar_date]`, legend `dim_channel[channel_name]`
- Matrix: `dim_queue[queue_name]` × Service Level %, Abandon Rate %
- Column chart: Contacts by `dim_time_of_day[hour_of_day]` (peak hours), or by `daypart`
- Slicers: date range, `dim_channel[channel_name]`

**Page 2 – Clinical outcomes**
- Cards: Encounters · Remote Resolution % · Emergency Escalation % · Avg Consultation (min)
- Bar chart: Encounters and Remote Resolution % by `dim_service_line[service_line_name]`
- Stacked bar: Encounters by `dim_triage_level[triage_name]` and `dim_disposition[care_setting]`
- Table: top 10 `dim_diagnosis[description]` by Weighted Diagnoses
- Column chart: Encounters by `fact_encounter[age_band]`

**Page 3 – Patients and regions**
- Map or bar: Encounters by `dim_patient[canton_name]`
- Donut: Encounters by `dim_patient[language_region]`
- Line: Encounters by date for weekday vs `dim_date[is_weekend_or_holiday]`

Title each page, add a text box "Source: telecare_dev.marts · refreshed after the daily load".

## P5. Save and publish evidence (5 min)

- Save as `powerbi/telecare_overview.pbix` in the repo (commit through a PR as usual).
- **File → Export → PDF** → `docs/img/powerbi_report.pdf`, and screenshots of pages 1–2 as
  `docs/img/powerbi_operations.png`, `docs/img/powerbi_clinical.png`.
- (Optional, needs a Power BI licence) Publish to the Power BI service and schedule a refresh after
  the dbt job – in a real deployment the refresh would be triggered by the orchestration.
