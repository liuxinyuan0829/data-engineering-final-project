```mermaid
flowchart TD
    A[("📦 Kaggle Dataset\nDota 2 Pro League Matches 2023\nmain_metadata.csv · Leagues.csv")]

    subgraph Ingestion["🐍 Python Ingestion Scripts"]
        B["ingest_dota2_data.py\nHistorical bulk load\n2022 – 2025"]
        C["prefect/flows/ingest_monthly.py\nMonthly scheduled updates\n2026+  (Prefect orchestration)"]
    end

    D[("☁️ Google Cloud Storage\nBucket: de-dota2\nraw/{year}/{month}/main_metadata.csv\nraw/Constant/Leagues.csv")]

    subgraph BQ["🏛️ BigQuery  (theta-carving-486822-c0)"]
        E["External Tables\nmain_metadata · Leagues"]
        F["Materialized Tables"]
        E -->|"Materialize"| F
    end

    subgraph DBT["🔧 dbt  (dbt_dota)"]
        G["Staging Layer\nstg_leagues\nstg_main_metadata"]
        H["Marts Layer\ndim_leagues  (table)\nfact_main_metadata  (incremental)"]
        G -->|"Transform & join"| H
    end

    I[("📊 Looker Studio Dashboard\nMatch duration trends\nRadiant vs Dire win rates\nMatch volume by league tier")]

    A --> B
    A --> C
    B -->|"Upload CSV chunks"| D
    C -->|"Monthly upload + retry logic"| D
    D -->|"Register as external table"| E
    F -->|"Query"| G
    H -->|"Connect"| I