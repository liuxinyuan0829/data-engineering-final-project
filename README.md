# data-engineering-final-project

I would like to build a data engineering project to import dataset from a kaggle data source to google clound storage. 

step 1: use terraform to create GCS bucket and dataset. the credetials to gcp is stored in the  credetials folder, the settings of terraform is stored in terraform folder
step 2: use python script ingest_dota2_data.py to upload the file from kaggle data source to the gcp project. use Prefect as Orchestration for such purpose
step3: use dbt as transformation tool to process the data and create some data models
step4: display the data in dashboards, for example use powerbi




## Prefect Flow Structure


```mermaid
flowchart TB
    subgraph CLI["__main__ CLI Entry Points"]
        A1["python ingest_monthly.py"]
        A2["python ingest_monthly.py 2026 4"]
        A3["python ingest_monthly.py backfill"]
        A4["python ingest_monthly.py leagues"]
    end

    A1 -->|"default: prev month"| F1
    A2 -->|"year=2026, month=4"| F1
    A3 --> F2
    A4 --> F3

    subgraph F1["Flow: ingest-dota2-monthly"]
        direction TB
        M1["Resolve target month\n(default: previous month)"]
        M2["download_file\n{YYYYMM}/main_metadata.csv"]
        M3["upload_to_gcs\nraw/{YYYYMM}/main_metadata.csv"]
        M4["download_file\nConstants/Constants.Leagues.csv"]
        M5["upload_to_gcs\nraw/Constant/Leagues.csv"]
        M1 --> M2 --> M3 --> M4 --> M5
    end

    subgraph F2["Flow: backfill-dota2-monthly"]
        direction TB
        B1["Build target month list\n202601 → 202603"]
        B2["Loop: for each YYYYMM"]
        B3["download_file\n{YYYYMM}/main_metadata.csv"]
        B4["upload_to_gcs\nraw/{YYYYMM}/main_metadata.csv"]
        B5["download_file\nConstants/Constants.Leagues.csv"]
        B6["upload_to_gcs\nraw/Constant/Leagues.csv"]
        B1 --> B2 --> B3 --> B4
        B4 -->|"next month"| B2
        B4 -->|"all done"| B5 --> B6
    end

    subgraph F3["Flow: ingest-constants-leagues"]
        direction TB
        L1["download_file\nConstants/Constants.Leagues.csv"]
        L2["upload_to_gcs\nraw/Constant/Leagues.csv"]
        L1 --> L2
    end

    subgraph Tasks["Shared Prefect Tasks"]
        T1["download_file(path)\nkagglehub individual file download\nretries: 2"]
        T2["upload_to_gcs(local, blob)\nGCS upload + verification\nretries: 2"]
    end

    subgraph Infra["Infrastructure"]
        K["Kaggle API\nkagglehub"]
        G["GCS Bucket\nde-dota2"]
    end

    T1 -.-> K
    T2 -.-> G
```
