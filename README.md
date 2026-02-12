# Bike Share Analytics Pipeline

## Overview

This project implements a production-style data engineering pipeline for analyzing public bike share ride data using:

- Snowflake as the cloud data warehouse
- dbt Core for data transformation
- Dagster for orchestration (schedules + sensors)

The pipeline follows a layered warehouse architecture:

RAW → STAGING → INTERMEDIATE → MART

It is designed to be reproducible, modular, and automation-ready.

---

## Architecture

### High-Level Flow

1. Hourly schedule triggers ingestion of raw CSV data into Snowflake (RAW schema).
2. A Dagster sensor detects successful ingestion.
3. dbt executes transformation models (run + test).
4. Incremental mart tables are updated.
5. Data quality tests validate structural and business rules.

### Layered Data Model

- **RAW Schema**
  - Stores ingested bike ride data from CSV
  - Append-safe ingestion with ingestion timestamp

- **STAGING Layer (stg_*)**
  - Column standardization
  - Type casting
  - Data cleaning
  - Business rule enforcement (e.g., valid time sequence)

- **INTERMEDIATE Layer (int_*)**
  - Enrichment logic
  - Derived columns
  - Aggregation preparation

- **MART Layer (fact_*, dim_*)**
  - Analytics-ready tables
  - Incremental fact models
  - Aggregated business metrics

---

## Orchestration Design

### Schedule-Based Ingestion

Dagster schedule:
0 * * * *


Runs hourly:
- Executes `bike_ingestion_job`
- Loads raw data into Snowflake

### Sensor-Driven Transformation

A Dagster sensor:
- Monitors successful ingestion runs
- Triggers `bike_full_pipeline`
- Executes `dbt build` (models + tests)

This ensures:
- Time-based ingestion
- Event-driven transformation
- Decoupled execution stages

---

## Data Quality

### Schema Tests

Implemented using dbt schema tests:
- not_null
- unique
- accepted_values
- relationships

### Singular Tests

Custom business rule validations:
- Trip duration must be positive
- Stop time must be after start time
- No future ride timestamps

Staging layer enforces corrections where appropriate.

---

## Incremental Modeling

The mart layer uses incremental models:

- Prevents full table rebuilds
- Processes only new ride dates
- Reduces compute cost
- Improves scalability

---

## Snowflake Configuration

- Dedicated warehouse with auto suspend
- Separate schemas for RAW, STAGING, MART
- Role-based access control
- Append-safe ingestion strategy

---

## Project Structure

Bike-Share-Analytics/
│
├── bike_share_dagster/
│ ├── assets.py
│ ├── definitions.py
│
├── dbt/
│ └── bike_share_dbt/
│ ├── models/
│ │ ├── staging/
│ │ ├── intermediate/
│ │ └── marts/
│ ├── tests/
│ ├── macros/
│ └── dbt_project.yml
│
├── data/
│ └── raw_csv/
│
├── requirements.txt
└── README.md


---

## How to Run Locally

### 1. Clone the repository

git clone <repository_url>
cd Bike-Share-Analytics


### 2. Create virtual environment

python -m venv venv
venv\Scripts\activate


### 3. Install dependencies

pip install -r requirements.txt


### 4. Configure Environment Variables

Create a `.env` file:

SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=RAW


### 5. Generate dbt Manifest

cd dbt/bike_share_dbt
dbt compile


### 6. Start Dagster

From project root:

dagster dev


Access UI at:
http://127.0.0.1:3000


---

## Business Insights Enabled

The mart layer enables:

- Overall ride activity trends (daily, weekly, monthly)
- Peak usage hours and seasonal patterns
- User behavior analysis (Subscriber vs Customer)
- Demographic analysis by birth year and gender
- Station popularity and route frequency
- Bike utilization and maintenance insights
- Ride duration distribution analysis

---

## Production Considerations

This implementation includes:

- Incremental modeling strategy
- Event-driven orchestration
- Layered warehouse architecture
- Business-rule validation via singular tests
- Reproducible local setup
- Version-controlled codebase