# Dota 2 Pro Match Analytics — Data Engineering Final Project

## Problem Statement

Dota 2 is a 2013 multiplayer online battle arena (MOBA) video game by Valve, played in matches between two teams of five players — **Radiant** and **Dire** — each defending their own base on an asymmetric map.

> *"Dota 2 is played in matches between two teams of five players, with each team occupying and defending their own separate base on the map. Each of the ten players independently controls a powerful character known as a 'hero' that all have unique abilities and differing styles of play."*
> — [Wikipedia](https://en.wikipedia.org/wiki/Dota_2)
> — please attach picture from [pictures/do]


After years of playing, two observable trends motivated this project:

1. **Increasing match duration** — as the map has grown larger and more resources have been introduced over successive patches, professional matches appear to run longer.
2. **Side imbalance** — due to asymmetric resource distribution on the map, the Radiant and Dire sides may have measurably different win rates.

The goal of this project is to build an end-to-end data pipeline that ingests professional match data, transforms it into analytics-ready models, and surfaces findings in an interactive dashboard.

---

## Data Source

**Kaggle:** [Dota 2 Pro League Matches 2023](https://www.kaggle.com/datasets/bwandowando/dota-2-pro-league-matches-2023)

- Professional match data from 2016 to present
- Updated on a weekly basis
- Key files: `main_metadata.csv` (per year/month, it stores the metadata per match, for example start_date, duration, win/loss, match_id, league_id etc)
           `Constants/Constants.Leagues.csv` (the list of leagues and tiers, updated actively)
     
---


## Architecture & Methodology

```mermaid
flowchart TD
    A[("�️ Kaggle\nDota 2 Pro League Matches 2023\nmain_metadata.csv · Leagues.csv")]:::kaggle

    subgraph Infra["☁️ Cloud Infrastructure"]
        T["🏗️ Terraform\nProvisions GCS bucket\n& BigQuery dataset as code"]:::terraform
    end

    subgraph Ingestion["Data Ingestion"]
        B["🐍 Python\ningest_dota2_data.py\nHistorical bulk load  2022–2025"]:::python
        C["🟣 Prefect\nprefect/flows/ingest_monthly.py\nScheduled monthly flow  2026+"]:::prefect
    end

    D[("🪣 Google Cloud Storage\nBucket: de-dota2\nraw/{year}/{month}/main_metadata.csv\nraw/Constant/Leagues.csv")]:::gcs

    subgraph BQ["Data Warehouse"]
        E["🔵 BigQuery\nExternal Tables\nmain_metadata · Leagues"]:::bigquery
        F["🔵 BigQuery\nMaterialized Tables"]:::bigquery
        E -->|"Materialize"| F
    end

    subgraph DBT["Data Transformation"]
        G["🟠 dbt  —  Staging Layer\nstg_leagues · stg_main_metadata\nClean · Cast · Deduplicate"]:::dbt
        H["🟠 dbt  —  Marts Layer\ndim_leagues · fact_main_metadata\nDimension & Incremental Fact Table"]:::dbt
        G -->|"Transform & join"| H
    end

    I[("📊 Looker Studio\nMatch duration trends\nRadiant vs Dire win rates\nMatch volume by league tier")]:::looker

    T -.->|"Provisions"| D
    T -.->|"Provisions"| E
    A --> B
    A --> C
    B -->|"Upload CSV chunks"| D
    C -->|"Monthly upload + retry logic"| D
    D -->|"Register as external table"| E
    F -->|"Query"| G
    H -->|"Connect"| I

    classDef kaggle     fill:#20BEFF,stroke:#20BEFF,color:#fff
    classDef terraform  fill:#7B42BC,stroke:#7B42BC,color:#fff
    classDef python     fill:#3776AB,stroke:#3776AB,color:#fff
    classDef prefect    fill:#6E56CF,stroke:#6E56CF,color:#fff
    classDef gcs        fill:#EA4335,stroke:#EA4335,color:#fff
    classDef bigquery   fill:#4285F4,stroke:#4285F4,color:#fff
    classDef dbt        fill:#FF694A,stroke:#FF694A,color:#fff
    classDef looker     fill:#34A853,stroke:#34A853,color:#fff
```

### Technology Choices

| Layer | Tool | Purpose |
|---|---|---|
| Cloud Infrastructure | **Terraform** + **Google Cloud** | Provision GCS bucket and BigQuery dataset as code |
| Data Lake | **Google Cloud Storage (GCS)** | Store raw CSV files partitioned by year/month |
| Workflow Orchestration | **Prefect** | Schedule and monitor monthly ingestion tasks |
| Data Warehouse | **BigQuery** | Store and query structured match data at scale |
| Data Transformation | **dbt** | Model, test, and document analytics-ready tables |
| Dashboard | **Looker Studio** | Visualize trends and findings |



## Data Pipeline Detail

### 1. Infrastructure Provisioning (Terraform)

Terraform provisions all cloud resources declaratively, ensuring reproducibility.

- **GCS bucket** `de-dota2` (region: US) with lifecycle rules for incomplete multipart uploads
- **BigQuery dataset** `main_metadata` in project `theta-carving-486822-c0`
- Credentials are loaded from `credentials/gcs_service_account.json`

```bash
cd terraform
terraform init
terraform apply
```

### 2. Data Ingestion

**Historical load (2022–2025):** `ingest_dota2_data.py`

Downloads each year's `main_metadata.csv` from Kaggle via `kagglehub` and uploads it to GCS at `raw/{year}/main_metadata.csv`. Files are uploaded in 8 MB chunks and verified after upload.

```bash
# Run manually for a historical bulk load
python ingest_dota2_data.py
```
**Ongoing monthly load (2026+):** `prefect/flows/ingest_monthly.py`

A Prefect flow scheduled to run monthly. It downloads the previous month's match file and the latest leagues constants file, then uploads both to GCS. Tasks include retry logic (2 retries, 30-second delay) for resilience.

```bash
# Run manually for a specific month (positional args: year month)
python prefect/flows/ingest_monthly.py 2026 4

# Backfill multiple months
python prefect/flows/ingest_monthly.py backfill

# Refresh leagues constants only
python prefect/flows/ingest_monthly.py leagues
```

**Prefect flow diagram:**

```mermaid
flowchart TD
    START(["▶️ Run ingest_monthly.py"]):::entry

    START --> ARG{{"CLI args?"}}:::decision

    ARG -->|"argv[1] == 'backfill'"| BF_FLOW
    ARG -->|"argv[1] == 'leagues'"| LG_FLOW
    ARG -->|"argv[1] & argv[2] == year month"| IM_FLOW_ARGS
    ARG -->|"no args"| IM_FLOW_DEFAULT

    subgraph IM["🟣 Flow: ingest-dota2-monthly"]
        IM_FLOW_ARGS["ingest_monthly(year, month)"]:::prefect
        IM_FLOW_DEFAULT["ingest_monthly()\n→ default to previous month"]:::prefect
        IM_FLOW_ARGS & IM_FLOW_DEFAULT --> IM_DL
        IM_DL["📥 Task: download_file\nyyyymm/main_metadata.csv\n↺ retries=2, delay=30s"]:::task
        IM_DL --> IM_UL["📤 Task: upload_to_gcs\nraw/yyyymm/main_metadata.csv\n↺ retries=2, delay=10s"]:::task
        IM_UL --> IM_LG_TRY{{"Try: download leagues?"}}:::decision
        IM_LG_TRY -->|"success"| IM_LG_DL["📥 Task: download_file\nConstants/Constants.Leagues.csv"]:::task
        IM_LG_DL --> IM_LG_UL["📤 Task: upload_to_gcs\nraw/Constant/Leagues.csv"]:::task
        IM_LG_TRY -->|"exception → warn & skip"| IM_DONE
        IM_LG_UL --> IM_DONE(["✅ Done"]):::success
    end

    subgraph BF["🟣 Flow: backfill-dota2-monthly"]
        BF_FLOW["backfill(start_year, start_month,\nend_year, end_month)"]:::prefect
        BF_FLOW --> BF_LOOP["🔁 Loop over each yyyymm"]:::loop
        BF_LOOP --> BF_TRY{{"Try download & upload"}}:::decision
        BF_TRY -->|"success"| BF_DL["📥 Task: download_file\nyyyymm/main_metadata.csv"]:::task
        BF_DL --> BF_UL["📤 Task: upload_to_gcs\nraw/yyyymm/main_metadata.csv"]:::task
        BF_UL --> BF_NEXT["next month →"]:::loop
        BF_NEXT --> BF_LOOP
        BF_TRY -->|"exception → warn & skip"| BF_NEXT
        BF_UL --> BF_LG_TRY{{"Try: download leagues?"}}:::decision
        BF_LG_TRY -->|"success"| BF_LG["📥📤 download + upload\nConstants.Leagues.csv"]:::task
        BF_LG_TRY -->|"exception → warn"| BF_DONE
        BF_LG --> BF_DONE(["✅ Backfill complete"]):::success
    end

    subgraph LG["🟣 Flow: ingest-constants-leagues"]
        LG_FLOW["ingest_leagues()"]:::prefect
        LG_FLOW --> LG_DL["📥 Task: download_file\nConstants/Constants.Leagues.csv"]:::task
        LG_DL --> LG_UL["📤 Task: upload_to_gcs\nraw/Constant/Leagues.csv"]:::task
        LG_UL --> LG_DONE(["✅ Done"]):::success
    end

    classDef entry    fill:#1e1e2e,stroke:#cdd6f4,color:#cdd6f4
    classDef prefect  fill:#6E56CF,stroke:#6E56CF,color:#fff
    classDef task     fill:#4C4F8A,stroke:#9399b2,color:#fff
    classDef loop     fill:#313244,stroke:#89b4fa,color:#cdd6f4
    classDef decision fill:#45475a,stroke:#f9e2af,color:#f9e2af
    classDef success  fill:#40A02B,stroke:#40A02B,color:#fff
```


**Ingested tables in GCS:** :

![Google Cloud Storage](pictures/google%20cloud%20storage.PNG)

### 3. BigQuery — External & Materialized Tables

Raw CSV files in GCS are registered as **external tables** in BigQuery, then materialized into native tables for query performance.

```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `theta-carving-486822-c0.dbt_dota.external_main_metadata`
OPTIONS (
  format = 'csv',
  uris = ['gs://de-dota2/raw/2024/main_metadata.csv','gs://de-dota2/raw/2025/main_metadata.csv','gs://de-dota2/raw/20260*/main_metadata.csv']
);

CREATE OR REPLACE EXTERNAL TABLE `theta-carving-486822-c0.dbt_dota.external_leagues`
OPTIONS (
  format = 'csv',
  uris = ['gs://de-dota2/raw/Constant/Leagues.csv']
);


-- Creating materialized table for better query performance
CREATE OR REPLACE TABLE `theta-carving-486822-c0.dbt_dota.main_metadata` AS
SELECT * FROM `theta-carving-486822-c0.dbt_dota.external_main_metadata`;

CREATE OR REPLACE TABLE `theta-carving-486822-c0.dbt_dota.leagues` AS
SELECT * FROM `theta-carving-486822-c0.dbt_dota.external_leagues`;
```

**processed tables in data warehouse:** :

![BigQuery](pictures/BIGQUERY.PNG)

### 4. Data Transformation (dbt)

The dbt project `dbt_dota` (located in `dbt_dota/`) applies a two-layer transformation strategy:

**Staging layer** (`models/staging/`)

| Model | Description |
|---|---|
| `stg_leagues` | Cleans and casts raw league data; excludes null tiers |
| `stg_main_metadata` | Cleans and casts raw match data; deduplicates by `match_id` using `ROW_NUMBER()` |

**Marts layer** (`models/marts/`)

| Model | Materialization | Description |
|---|---|---|
| `dim_leagues` | Table | Deduplicated dimension of leagues with tier classifications |
| `fact_main_metadata` | Incremental (merge on `match_id`) | Fact table joining match metrics with league dimension; incrementally updated on `start_datetime` |

**Data tests** are defined in `schema.yml` files for both layers, covering uniqueness, nullability, and referential integrity between fact and dimension tables.

```bash
cd dbt_dota
dbt run
dbt test
```

### 5. Dashboard (Looker Studio)

The final dashboard connects to the `fact_main_metadata` and `dim_leagues` BigQuery tables to surface:

- Trend of average match duration over patch versions
- Radiant vs. Dire win rate comparison
- Match volume by league tier over time

**[View Dashboard](https://datastudio.google.com/reporting/aff70da2-c27a-4443-aca7-ad8a72cb49eb)**

![Dashboard](pictures/dashboard.PNG)

#### Key Findings

- **Match duration is increasing** — The average duration of professional matches has grown over time, consistent with the hypothesis that a larger map and more in-game resources lead to longer games.
- **Match volume declined from early 2026** — The number of recorded matches dropped noticeably from the start of 2026. The exact cause is unclear and could reflect factors such as tournament scheduling, data availability, or shifts in the professional scene.
- **Radiant has a slightly higher win rate** — The Radiant side wins marginally more often than Dire, suggesting a mild map-side advantage that may be worth investigating further.

---

## Quick Start Reproducibility

**Prerequisites:** Python environment with dependencies installed (`pyproject.toml`), GCP account with billing enabled, Kaggle account.

```bash
# Step 1 — Clone the repository
git clone https://github.com/liuxinyuan0829/data-engineering-final-project.git
cd data-engineering-final-project

# Step 2 — Set up GCP IAM service account
# 1. Go to https://console.cloud.google.com/iam-admin/serviceaccounts
# 2. Create a service account (e.g. "de-dota2-sa") with the following roles:
#      - Storage Admin         (read/write GCS)
#      - BigQuery Admin        (create datasets, tables, run queries)
# 3. Create a JSON key for the service account
# 4. Download the key and save it to:
mkdir -p credentials
mv ~/Downloads/<your-key-file>.json credentials/gcs_service_account.json

# Step 3 — Configure environment variables
cp .env.example .env
# Edit .env and fill in your values:
#   KAGGLE_USERNAME  — your Kaggle username
#   KAGGLE_KEY       — your Kaggle API key (from https://www.kaggle.com/settings)
#   GCP_PROJECT, GCS_BUCKET, GOOGLE_APPLICATION_CREDENTIALS (pre-filled)

# Load environment variables
set -a && source .env && set +a

# Step 4 — Provision cloud infrastructure
cd terraform && terraform init && terraform apply && cd ..

# Step 5 — Bulk-load historical data (2022–2025)
python ingest_dota2_data.py

# Step 6 — Trigger monthly runs for 2026+

# Run for a specific month (positional args: year month)
python prefect/flows/ingest_monthly.py 2026 4

# Backfill multiple months
python prefect/flows/ingest_monthly.py backfill

# Refresh leagues constants only
python prefect/flows/ingest_monthly.py leagues

# Step 7 — Create BigQuery external + materialized tables
# Run the SQL statements in the BigQuery section above

# Step 8 — Run dbt transformations and tests
cd dbt_dota && dbt run && dbt test

# Step 9 — Open Looker Studio dashboard
# https://datastudio.google.com/reporting/aff70da2-c27a-4443-aca7-ad8a72cb49eb
```

---

## Project Structure

```
├── credentials/                  # GCP service account key (not committed)
├── terraform/                    # Infrastructure as code
│   ├── main.tf                   # GCS bucket + BigQuery dataset definitions
│   └── variables.tf              # Project, region, bucket, dataset variables
├── ingest_dota2_data.py          # Historical bulk ingestion (2022–2025)
├── prefect/
│   └── flows/
│       └── ingest_monthly.py     # Scheduled monthly ingestion flow (2026+)
├── dbt_dota/                     # dbt transformation project
│   ├── models/
│   │   ├── staging/              # stg_leagues, stg_main_metadata
│   │   └── marts/                # dim_leagues, fact_main_metadata
│   └── dbt_project.yml
└── README.md
```