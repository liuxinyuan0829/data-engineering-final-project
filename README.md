# Dota 2 Pro Match Analytics — Data Engineering Final Project

## Problem Statement

Dota 2 is a 2013 multiplayer online battle arena (MOBA) video game by Valve, played in matches between two teams of five players — **Radiant** and **Dire** — each defending their own base on an asymmetric map.

> *"Dota 2 is played in matches between two teams of five players, with each team occupying and defending their own separate base on the map. Each of the ten players independently controls a powerful character known as a 'hero' that all have unique abilities and differing styles of play."*
> — [Wikipedia](https://en.wikipedia.org/wiki/Dota_2)

After years of playing, two observable trends motivated this project:

1. **Increasing match duration** — as the map has grown larger and more resources have been introduced over successive patches, professional matches appear to run longer.
2. **Side imbalance** — due to asymmetric resource distribution on the map, the Radiant and Dire sides may have measurably different win rates.

The goal of this project is to build an end-to-end data pipeline that ingests professional match data, transforms it into analytics-ready models, and surfaces findings in an interactive dashboard.

---

## Architecture & Methodology

```
Kaggle Dataset
     │
     ▼
[Python Ingestion Scripts]
 ingest_dota2_data.py      ← historical bulk load (2022–2025)
 prefect/flows/ingest_monthly.py ← monthly scheduled updates (2026+)
     │
     ▼
Google Cloud Storage (GCS)
  Bucket: de-dota2
  Layout: raw/{year}/{month}/main_metadata.csv
          raw/Constant/Leagues.csv
     │
     ▼
BigQuery (Data Warehouse)
  External tables → Materialized tables
  Dataset: main_metadata (project: theta-carving-486822-c0)
     │
     ▼
dbt (Data Transformation)
  Staging layer  → stg_leagues, stg_main_metadata
  Marts layer    → dim_leagues, fact_main_metadata
     │
     ▼
Looker Studio (Dashboard)
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

---

## Data Source

**Kaggle:** [Dota 2 Pro League Matches 2023](https://www.kaggle.com/datasets/bwandowando/dota-2-pro-league-matches-2023)

- Professional match data from 2016 to present
- Updated on a weekly basis
- Key files: `main_metadata.csv` (per year/month), `Constants/Constants.Leagues.csv`

---

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

![Google Cloud Storage](pictures/google%20cloud%20storage.PNG)

```bash
python ingest_dota2_data.py
```

**Ongoing monthly load (2026+):** `prefect/flows/ingest_monthly.py`

A Prefect flow scheduled to run monthly. It downloads the previous month's match file and the latest leagues constants file, then uploads both to GCS. Tasks include retry logic (2 retries, 30-second delay) for resilience.

```bash
# Run manually for a specific month
python -m prefect.flows.ingest_monthly --year 2026 --month 3
```

### 3. BigQuery — External & Materialized Tables

Raw CSV files in GCS are registered as **external tables** in BigQuery, then materialized into native tables for query performance.

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

**Prerequisites:** GCP service account key at `credentials/gcs_service_account.json`, Kaggle API credentials configured, Python environment with dependencies installed (`pyproject.toml`).

```bash
# Step 1 — Provision cloud infrastructure
cd terraform && terraform init && terraform apply

# Step 2 — Bulk-load historical data (2022–2025)
python ingest_dota2_data.py

# Step 3 — (Optional) Trigger a manual monthly run for 2026+
python -m prefect.flows.ingest_monthly

# Step 4 — Create BigQuery external + materialized tables
# (attach BigQuery setup screenshots here)

# Step 5 — Run dbt transformations and tests
cd dbt_dota && dbt run && dbt test

# Step 6 — Open Looker Studio dashboard
# (attach dashboard URL here)
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