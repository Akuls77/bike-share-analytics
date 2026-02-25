# Bike Share Analytics Platform

A layered data pipeline built using:

* Snowflake – Cloud Data Warehouse
* dbt – SQL-based transformations (RDS → CDS → DDS → IDS)
* Dagster – Orchestration, scheduling, and monitoring
* Python – Raw data ingestion

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

---

## Data Layering Strategy

### 1. RDS – Raw Data Store

Purpose
Stores raw data exactly as received from the source system.

Technology

* Python ingestion asset
* Snowflake table: `RAW_BIKE_RIDES`

Dagster Group
`rds`

Asset

* `load_raw_bike_rides`

---

### 2. CDS – Cleansed Data Store

Purpose
Standardizes and enriches raw data before dimensional modeling.

Typical Transformations

* Data type casting
* Null handling
* Column normalization
* Derived fields (age, season, weekend flag)

Technology

* dbt models tagged `cds`

Dagster Group
`cds`

Each dbt model is registered as an individual Dagster asset.

---

### 3. DDS – Dimensional Data Store

Purpose
Implements star schema modeling for analytical workloads.

Components

* `dds_fact_rides`
* `dds_dim_user`
* `dds_dim_bike`
* `dds_dim_station`
* `dds_dim_route`

Key Features

* Surrogate key generation
* Distance calculation (km)
* Route categorization

Technology

* dbt models tagged `dds`

Dagster Group
`dds`

This layer is optimized for analytical queries and BI tools.

---

### 4. IDS – Insights Data Store

Purpose
Provides business-ready aggregated datasets.

Examples

* `ids_bike_utilization`
* `ids_station_performance`
* `ids_user_behavior`
* `ids_daily_metrics`

Key Features

* Total and average measures
* Business-oriented aggregations

Technology

* dbt models tagged `ids`

Dagster Group
`ids`

This layer supports reporting and dashboard consumption.

---

## Orchestration Strategy

Orchestration is handled using Dagster with strict layer sequencing and monitoring.

Execution Model:

1. A 2-hour schedule triggers the pipeline.
2. RDS ingestion asset executes.
3. CDS layer builds (`tag:cds`).
4. On successful completion of CDS:

   * DDS layer is triggered via run status sensor.
5. On successful completion of DDS:

   * IDS layer is triggered via run status sensor.
6. dbt tests execute automatically during each build.
7. Analytics tables are refreshed.

A global failure sensor monitors all runs and sends email alerts on pipeline failure.

---

## Asset Structure in Dagster

Dagster registers:

* RDS group → Python ingestion asset
* CDS group → All dbt CDS models
* DDS group → All dbt DDS models
* IDS group → All dbt IDS models

Each dbt model becomes an individual Dagster asset.

Dependencies are automatically derived from the dbt manifest.

---

## Scheduling and Sensors

### Schedule

* Pipeline runs every 2 hours.
* Configured via `ScheduleDefinition`.

### Sensors

* CDS success triggers DDS.
* DDS success triggers IDS.
* Global failure sensor sends email alerts.
* No layer runs unless the previous layer succeeds.

This ensures both scheduled execution and event-driven chaining.

---

## Technology Stack

| Tool      | Purpose                    |
| --------- | -------------------------- |
| Snowflake | Data Warehouse             |
| dbt       | SQL Transformations        |
| Dagster   | Orchestration & Monitoring |
| Python    | Raw Ingestion              |

---

## Setup Instructions

### 1. Clone the Repository

```
git clone <your-repository-url>
cd Bike-Share-Analytics
```

---

### 2. Create Virtual Environment

#### Windows

```
python -m venv venv
venv\Scripts\activate
```

#### macOS / Linux

```
python3 -m venv venv
source venv/bin/activate
```

---

### 3. Install Dependencies

Make sure `requirements.txt` exists at the project root.

```
pip install -r requirements.txt
```

This installs:

* dagster
* dagster-dbt
* dbt-core
* dbt-snowflake
* snowflake-connector

---

### 4. Build dbt Project (First Time)

```
cd dbt/bike_share_dbt
dbt clean
dbt build
```

---

### 5. Start Dagster

```
cd bike_share_dagster
dagster dev
```

Open in browser:

```
http://127.0.0.1:3000
```