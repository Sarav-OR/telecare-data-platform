# Decision log

One line per decision or incident: what happened, why, what we chose. The backbone for the
README "trade-offs" section and for onboarding new team members.

| # | Date | Area | What happened / what we decided | Why |
|---|---|---|---|---|
| 1 | 25 Sep | Azure SQL | Loader login timeout (error 258) → retry 4× / 120 s timeout | Free-offer serverless DB auto-pauses; first login wakes it (~1 min) |
| 2 | 25 Sep | Databricks | Cluster policy/job no longer set tag `project` | Workspace Azure tags propagate to clusters; duplicate key was renamed and failed policy validation |
| 3 | 25 Sep | Governance | Column mask moved from bronze/silver to the vault satellite; bronze/silver = restricted zone + `pii` tags | Dedicated (single-user) job clusters cannot read or MERGE tables with column masks |
| 4 | 26 Sep | dbt | Post-hook written as a string `"{{ apply_pii_mask(...) }}"` | A macro call inside `config()` renders at parse time, before `this` has its final schema |
| 5 | 26 Sep | dbt Cloud | GitHub app scoped to one repo; fixed "account already linked" | Least privilege; the app is needed for PR-triggered CI |
| 6 | 26 Sep | dbt Cloud | Freshness non-blocking until ADF runs, then a blocking command | Silver older than 26 h would correctly stop the build before daily loads existed |
| 7 | 26 Sep | CI | CI job target name left `default` | `prod`/`ci` targets drop the dev prefix → CI would write into production schemas |
| 8 | 26 Sep | ADF | Metadata-driven pipelines (ctl tables), batch clock, watermark moved after copy | New feed = one row; failed day is re-runnable; no skipped data |
| 9 | 26 Sep | Ingestion | `recover_type_drift`: values rescued by Auto Loader are cast back | ADF may write SQL `date`/`tinyint` with other Parquet types than the bootstrap files |
| 10 | 26 Sep | ADF | Managed identity for storage, SQL and the Databricks Jobs API; job permission CAN_MANAGE_RUN in the bundle | No secrets; least privilege as code |
