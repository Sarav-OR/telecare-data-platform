# Day 1 – Azure foundation setup (Track A)

Goal: create every Azure resource the TeleCare data platform needs in **one resource group**,
wire up identities and permissions, and prove Databricks can read the storage account —
**without starting a single cluster**.

Estimated time: 1.5–2 hours. Do the steps in order; each ends with a ✅ check.

> **Naming rule.** Storage account, Key Vault, SQL server and Data Factory names must be
> globally unique. If a name is taken, add a short suffix (e.g. `sttelecaredevchn01`) and
> note it in the table at the end.

> **Do NOT (these cost money or add complexity):**
> - start or create any Databricks cluster today
> - enable Microsoft Defender for SQL / for Cloud trials
> - choose geo-redundant storage (GRS/RA-GRS)
> - enable "Managed virtual network" in Data Factory

---

## A0. Before you start

- You need **Owner** (or Contributor + User Access Administrator) on the subscription,
  because several steps assign roles.
- Region for everything: **Switzerland North**.
- Tags to add on every resource (Tags tab): `project = telecare`, `env = dev`, `owner = sarav`.
  Tags make the cost report per project trivial.

---

## A1. Resource group

Portal → **Resource groups** → **Create**

| Setting | Value |
|---|---|
| Name | `rg-telecare-dev-chn` |
| Region | Switzerland North |

✅ The resource group appears in the list.

---

## A2. Budget alert

Portal → **Cost Management** → **Budgets** → **Add** (scope: `rg-telecare-dev-chn`)

| Setting | Value |
|---|---|
| Name | `bud-telecare-dev` |
| Reset period | Monthly |
| Amount | 20 (your billing currency) |
| Alert conditions | Actual 50 %, Actual 80 %, Actual 100 %, Forecasted 100 % |
| Alert recipients | your email |

Budgets **alert only, they do not stop resources** — the cluster policy and auto-stop settings
later are what actually prevent cost.

✅ Budget listed with 4 alert conditions.

---

## A3. Storage account (ADLS Gen2)

Portal → **Storage accounts** → **Create**

**Basics**

| Setting | Value |
|---|---|
| Resource group | `rg-telecare-dev-chn` |
| Name | `sttelecaredevchn` |
| Region | Switzerland North |
| Primary service | Azure Blob Storage or Azure Data Lake Storage Gen2 |
| Performance | Standard |
| Redundancy | **LRS** |

**Advanced**

| Setting | Value |
|---|---|
| **Enable hierarchical namespace** | ✅ **ON** (this is what makes it ADLS Gen2 – cannot be changed later) |
| Allow enabling anonymous access on containers | OFF |
| Minimum TLS version | 1.2 |
| Access tier | Hot |

**Networking:** Public access from all networks (acceptable for a demo; production would use
private endpoints — we document this). **Data protection:** defaults are fine.

After creation → **Data storage → Containers** → create 4 private containers:

| Container | Purpose |
|---|---|
| `vendor-drop` | Where source systems deliver files (ADF reads from here) |
| `landing` | Immutable copy written by ADF (Databricks reads from here) |
| `checkpoints` | Auto Loader schemas and streaming checkpoints |
| `lakehouse` | Managed storage for the Unity Catalog catalog (Delta tables) |

✅ 4 containers exist; the account overview shows **Hierarchical namespace: Enabled**.

---

## A4. Key Vault

Portal → **Key vaults** → **Create**

| Setting | Value |
|---|---|
| Name | `kv-telecare-dev-chn` |
| Region | Switzerland North |
| Pricing tier | Standard |
| Soft-delete retention | 7 days |
| Purge protection | Disabled (so names can be reused after deleting the project) |
| **Access configuration** | **Azure role-based access control** |

Then **Access control (IAM)** → **Add role assignment** → role **Key Vault Secrets Officer** →
assign to **yourself**.

> Gotcha: being Owner of the subscription does **not** let you read or write secrets in an
> RBAC vault. Without this role you'll get "not authorized" when adding a secret.

✅ Secrets → **Generate/Import** is clickable without an error.

---

## A5. Azure SQL database (simulates the clinical system / EHR)

Portal → **SQL databases** → **Create**. At the top of the form look for the banner
**"Want to try Azure SQL Database for free?"** → **Apply offer**.

**Basics**

| Setting | Value |
|---|---|
| Resource group | `rg-telecare-dev-chn` |
| Database name | `sqldb-telecare-ehr` |
| Server | **Create new** → name `sql-telecare-dev-chn`, location Switzerland North |
| Authentication method | **Use both SQL and Microsoft Entra authentication** |
| Microsoft Entra admin | yourself |
| SQL admin login | `sqladmin_telecare` + a strong password |
| **Free offer – behavior when limit is reached** | **Auto-pause the database until next month** (guarantees no charge) |

If the free-offer banner is not shown in Switzerland North, tell me before continuing — the
fallback is *General Purpose → Serverless*, min 0.5 vCores, **auto-pause after 1 hour**
(a few CHF at most for the week).

**Networking**

| Setting | Value |
|---|---|
| Connectivity method | Public endpoint |
| **Allow Azure services and resources to access this server** | **Yes** (Data Factory needs it) |
| Add current client IP address | Yes |
| Minimum TLS | 1.2 |

**Security:** Microsoft Defender for SQL → **Not now**.
**Additional settings:** Use existing data → None.

After creation: Key Vault → **Secrets** → **Generate/Import** → name `sql-admin-password`,
value = the SQL admin password.

✅ In the database → **Query editor**, sign in with Entra and run `SELECT @@VERSION;` successfully.

---

## A6. Data Factory (empty for now)

Portal → **Data factories** → **Create**

| Setting | Value |
|---|---|
| Name | `adf-telecare-dev-chn` |
| Region | Switzerland North |
| Version | V2 |
| Git configuration | **Configure Git later** (we connect the repo on Day 3) |
| Networking | Public endpoint; **Managed virtual network: OFF** |

A system-assigned managed identity is created automatically. An empty factory costs nothing.

✅ **Launch studio** opens the ADF authoring UI.

---

## A7. Databricks workspace + access connector

### A7.1 Workspace

Portal → **Azure Databricks** → **Create**

| Setting | Value |
|---|---|
| Workspace name | `dbw-telecare-dev-chn` |
| Region | Switzerland North |
| Pricing tier | **Trial (Premium – 14-Days Free DBUs)** if listed, otherwise **Premium** |
| Managed resource group name | `mrg-dbw-telecare-dev-chn` |
| Networking | Defaults (Secure cluster connectivity = Yes, own VNet = No) |

Deployment takes ~5 minutes. Afterwards open the managed resource group and note whether it
contains a **NAT gateway** (small hourly cost, removed when the project is deleted).

### A7.2 Access connector (identity Databricks uses to reach storage)

Portal → search **Access Connector for Azure Databricks** → **Create**

| Setting | Value |
|---|---|
| Name | `dbac-telecare-dev-chn` |
| Region | Switzerland North |
| Managed identity | System-assigned ON |

Copy its **Resource ID** from the Overview/Properties page (looks like
`/subscriptions/…/resourceGroups/rg-telecare-dev-chn/providers/Microsoft.Databricks/accessConnectors/dbac-telecare-dev-chn`).

### A7.3 Give the connector access to the storage account

Storage account `sttelecaredevchn` → **Access control (IAM)** → **Add role assignment**
→ role **Storage Blob Data Contributor** → Members: **Managed identity** → *Access Connector for
Azure Databricks* → `dbac-telecare-dev-chn`.

✅ The role assignment appears under Role assignments (it can take ~5 minutes to be effective).

---

## A8. Unity Catalog objects + SQL warehouse (inside Databricks)

Open the workspace. **Do not create a cluster** – everything below uses the UI or the SQL warehouse.

### A8.1 Check Unity Catalog
**Catalog** (left menu) → the Catalog Explorer should show a metastore and catalogs such as
`system` / `samples`. If you only see `hive_metastore`, stop and tell me.

### A8.2 Storage credential
Catalog → ⚙ / **External data** → **Credentials** → **Create credential**

| Setting | Value |
|---|---|
| Credential type | Azure Managed Identity |
| Name | `cred_telecare_dev` |
| Access connector ID | the Resource ID from A7.2 |

### A8.3 External locations
**External data** → **External locations** → **Create** (three times):

| Name | URL | Credential |
|---|---|---|
| `el_telecare_dev_landing` | `abfss://landing@sttelecaredevchn.dfs.core.windows.net/` | `cred_telecare_dev` |
| `el_telecare_dev_checkpoints` | `abfss://checkpoints@sttelecaredevchn.dfs.core.windows.net/` | `cred_telecare_dev` |
| `el_telecare_dev_lakehouse` | `abfss://lakehouse@sttelecaredevchn.dfs.core.windows.net/` | `cred_telecare_dev` |

Click **Test connection** on each → all checks green (Read / List / Write / Delete).

### A8.4 SQL warehouse (used by the checks below, by dbt Cloud and by Power BI)
**SQL Warehouses** → **Create SQL warehouse**

| Setting | Value |
|---|---|
| Name | `wh-telecare-dev` |
| Cluster size | **2X-Small** |
| Auto stop | **5 minutes** (if Serverless is not offered: type Pro, 10 minutes) |
| Scaling | min 1, max 1 |
| Type | **Serverless** |

### A8.5 Catalog
Open **SQL Editor**, select warehouse `wh-telecare-dev`, run:

```sql
CREATE CATALOG IF NOT EXISTS telecare_dev
  MANAGED LOCATION 'abfss://lakehouse@sttelecaredevchn.dfs.core.windows.net/telecare_dev'
  COMMENT 'TeleCare CH data platform - development';

-- storage check: must return without error (empty result is fine)
LIST 'abfss://landing@sttelecaredevchn.dfs.core.windows.net/';
```

Then leave the warehouse alone – it stops itself after 5 minutes.

✅ `telecare_dev` appears in Catalog Explorer and the `LIST` command succeeded.

---

## A9. Data Factory permissions

| Where | Role | Assign to (Managed identity → Data factory) |
|---|---|---|
| Storage account `sttelecaredevchn` → IAM | **Storage Blob Data Contributor** | `adf-telecare-dev-chn` |
| Key Vault `kv-telecare-dev-chn` → IAM | **Key Vault Secrets User** | `adf-telecare-dev-chn` |

Database access for ADF (a contained user for its managed identity) is created on Day 3 with a
script from the repo.

✅ Both role assignments listed.

---

## A10. GitHub repository

github.com → **New repository**

| Setting | Value |
|---|---|
| Name | `telecare-data-platform` |
| Visibility | **Private** for now (made public after a secrets review on Day 5) |
| Initialize | nothing (no README, no .gitignore) – the code will be pushed from the prepared repo |

---

## Send back when done

Copy this table into the chat with the actual values (only names/IDs, **never passwords or keys**):

| Item | Value |
|---|---|
| Storage account name | |
| Key Vault name | |
| SQL server name | |
| Free offer applied (yes/no) | |
| Databricks workspace URL (`https://adb-….azuredatabricks.net`) | |
| Pricing tier (Trial / Premium) | |
| Serverless SQL warehouse available (yes/no) | |
| NAT gateway in managed RG (yes/no) | |
| GitHub repo URL | |
| Any step that failed | |
