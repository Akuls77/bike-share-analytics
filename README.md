# Bike Share Analytics Platform

A production-style layered data pipeline built using:

- Snowflake – Data warehouse
- dbt – SQL-based transformations (RDS → CDS → DDS → IDS)
- Dagster – Orchestration and scheduling
- Python – Raw data ingestion

This project demonstrates a complete modern data engineering architecture using strict layer separation, idempotent ingestion, and success-based orchestration.

---

## Architecture Overview

Raw CSV  
↓  
RDS (Raw Data Store)  
↓  
CDS (Cleansed Data Store)  
↓  
DDS (Dimensional Data Store)  
↓  
IDS (Information Delivery Store)  

Each layer is independently modeled, versioned, tested, and orchestrated.

---

## Data Layering Strategy

### 1. RDS – Raw Data Store

Purpose  
Stores raw data exactly as received from the source system.

Technology  
- Python ingestion logic  
- Snowflake table: `RAW_BIKE_RIDES`

Dagster Group  
`rds`

Asset  
- `load_raw_bike_rides`

Design Characteristics  
- Ingestion is idempotent.
- If the table already exists, ingestion is skipped.
- Prevents duplicate loading.
- Designed for static source data.

---

### 2. CDS – Cleansed Data Store

Purpose  
Standardizes and cleans raw data before dimensional modeling.

Typical Transformations  
- Data type casting  
- Null handling  
- Column normalization  
- Basic enrichment  

Technology  
- dbt models tagged `cds`

Dagster Group  
`cds`

Each dbt model is automatically registered as a separate Dagster asset.

---

### 3. DDS – Data Domains Store

Purpose  
Implements star schema modeling for analytical workloads.

Technology  
- dbt dimensional models tagged `dds`

Dagster Group  
`dds`

This layer is optimized for BI consumption and analytical queries.

---

### 4. IDS – Insights Data Store

Purpose  
Provides business-ready aggregated and summary datasets.

Technology  
- dbt aggregation models tagged `ids`

Dagster Group  
`ids`

This layer supports reporting and dashboard consumption.

---

## Orchestration Strategy

Orchestration is handled using Dagster with strict layer sequencing.

Execution Model:

1. CDS layer runs daily after 2 hours.
2. On successful completion of CDS:
   - DDS layer is triggered.
3. On successful completion of DDS:
   - IDS layer is triggered.

This is implemented using success-based sensors.

---

## Asset Structure in Dagster

Dagster registers:

- RDS group → Python ingestion asset
- CDS group → All dbt CDS models
- DDS group → All dbt DDS models
- IDS group → All dbt IDS models

Each dbt model becomes an individual asset.

Dependencies are automatically derived from the dbt manifest.

---

## Scheduling and Sensors

### Schedule

- CDS job runs daily at 5:00 PM IST.
- Configured via Dagster ScheduleDefinition.

### Sensors

- CDS success triggers DDS.
- DDS success triggers IDS.
- Sensors use strict success-based checks.
- No layer runs unless the previous layer succeeds.

This provides both scheduled execution and event-driven chaining.

---

## Technology Stack

| Tool       | Purpose                    |
|------------|----------------------------|
| Snowflake  | Data Warehouse             |
| dbt        | SQL Transformations        |
| Dagster    | Orchestration              |
| Python     | Raw Ingestion              |

---

## Setup Instructions

### 1. Clone the Repository

git clone <your-repository-url>
cd Bike-Share-Analytics

---

### 2. Create Virtual Environment

#### Windows

python -m venv venv
venv\Scripts\activate

#### macOS / Linux

python3 -m venv venv
source venv/bin/activate

---

### 3. Install Dependencies from requirements.txt

Make sure `requirements.txt` exists at the project root.

pip install --upgrade pip
pip install -r requirements.txt


This will install:
- dagster
- dagster-dbt
- dbt-core
- dbt-snowflake
- snowflake-connector
- and other required dependencies

---

### 4. Build dbt Project (First Time)

cd dbt/bike_share_dbt
dbt clean
dbt build

---

### 5. Start Dagster

cd bike_share_dagster
dagster dev


Open in browser:

http://127.0.0.1:3000