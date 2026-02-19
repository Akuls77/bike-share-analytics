from pathlib import Path
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from dagster import asset
from dagster_dbt import dbt_assets, DbtCliResource

from .config import (
    DBT_PROJECT_DIR,
    DBT_PROFILES_DIR,
    RAW_DATA_PATH,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
)

# DBT RESOURCE
dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
)

# RDS LAYER
@asset(
    name="rds_raw_bike_rides",
    group_name="rds_layer",
)
def rds_raw_bike_rides():
    """
    Loads raw CSV into Snowflake RDS schema.
    No transformation performed here.
    """

    if not RAW_DATA_PATH.exists():
        raise FileNotFoundError(f"Data file not found at {RAW_DATA_PATH}")

    df = pd.read_csv(RAW_DATA_PATH).reset_index(drop=True)

    if df.empty:
        raise ValueError("CSV file is empty.")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="RDS",
    )

    cursor = conn.cursor()

    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS RDS")

        cursor.execute("DROP TABLE IF EXISTS RDS.RAW_BIKE_RIDES")

        write_pandas(conn, df, "RAW_BIKE_RIDES")

    finally:
        cursor.close()
        conn.close()

    return "RDS load successful"


# CDS LAYER
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:cds",
)
def cds_models(context, dbt: DbtCliResource):
    """
    Runs CDS layer models:
    - Cleaning
    - Type casting
    - Gender mapping
    - Age derivation
    """
    yield from dbt.cli(["build", "--select", "tag:cds"], context=context).stream()


# DDS LAYER
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:dds",
)
def dds_models(context, dbt: DbtCliResource):
    """
    Runs DDS layer:
    - Domain separation (ride, station, user, bike)
    """
    yield from dbt.cli(["build", "--select", "tag:dds"], context=context).stream()


# IDS LAYER
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:ids",
)
def ids_models(context, dbt: DbtCliResource):
    """
    Runs IDS layer:
    - Aggregations
    - Business insights
    - Reporting tables
    """
    yield from dbt.cli(["build", "--select", "tag:ids"], context=context).stream()
