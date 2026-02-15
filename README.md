# Bike Share Analytics Platform

## Overview

Bike Share Analytics is a modern data engineering pipeline built using:

- Dagster (Orchestration)
- dbt (Transformation)
- Snowflake (Data Warehouse)
- Pandas (Ingestion)

The platform ingests raw NYC Bike Share CSV data, loads it into Snowflake, and transforms it into analytics-ready marts using a layered architecture.

---

## Architecture

### Data Flow

1. Raw CSV ingestion via Dagster
2. Load into Snowflake RAW schema
3. dbt transforms data across:
   - STAGING layer
   - INTERMEDIATE layer
   - MART layer
4. Data quality tests executed via dbt
5. Orchestrated end-to-end using Dagster asset jobs

---

## Layered Data Model

### RAW Layer
- Ingests CSV data directly into Snowflake
- No transformations applied
- Schema: `RAW`

### STAGING Layer
- Cleans and standardizes columns
- Applies type casting
- Performs basic validation
- Schema: `STAGING`

### INTERMEDIATE Layer
- Enriches data
- Applies business logic
- Schema: `INTERMEDIATE`

### MART Layer
- Aggregated fact tables
- Analytics-ready datasets
- Schema: `MART`

---

## Data Quality

dbt tests include:

- Not null constraints
- Accepted value checks
- Logical validations (e.g., stop_time >= start_time)
- Business rule validations (e.g., no negative rides)

All transformations run with:

dbt build


---

## Running the Project

### 1. Activate Virtual Environment

venv\Scripts\activate


### 2. Run Dagster

dagster dev


Open:

http://127.0.0.1:3000


Materialize assets to run the full pipeline.

---

### 3. Run dbt Independently

From dbt project directory:

dbt run
dbt test
dbt build


---

## Snowflake Setup

Ensure the following:

- Correct Snowflake account credentials
- Profiles configured in:

C:\Users<your_username>.dbt\profiles.yml


- Schemas exist:
  - RAW
  - STAGING
  - INTERMEDIATE
  - MART

---

## Key Features

- End-to-end orchestration with Dagster
- Layered data modeling best practices
- Automated data validation
- Incremental models
- Clean separation of ingestion and transformation
- Production-style structure