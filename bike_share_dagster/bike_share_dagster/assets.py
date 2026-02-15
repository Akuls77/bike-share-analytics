from pathlib import Path
import os
import pandas as pd
import snowflake.connector

from dagster import asset
from dagster_dbt import dbt_assets, DbtCliResource


# DBT CONFIG
DBT_PROJECT_DIR = Path("C:/Bike-Share-Analytics/dbt/bike_share_dbt")
DBT_PROFILES_DIR = Path(os.path.expanduser("~")) / ".dbt"

dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
)

# RAW INGESTION ASSET
@asset(
    name="raw_bike_rides",
    group_name="raw_layer",
)
def raw_bike_rides():
    """
    Loads static CSV data into Snowflake RAW schema.
    This is ingestion only — no transformations.
    """

    DATA_FILE_PATH = Path(
        "C:/Bike-Share-Analytics/data/raw_csv/NYC-BikeShare-2015-2017-combined.csv"
    )

    if not DATA_FILE_PATH.exists():
        raise FileNotFoundError(f"CSV file not found at {DATA_FILE_PATH}")

    df = pd.read_csv(DATA_FILE_PATH).reset_index(drop=True)

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="RAW"
    )

    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW")

    # Let write_pandas handle table creation
    from snowflake.connector.pandas_tools import write_pandas

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name="RAW_BIKE_RIDES",
        auto_create_table=True,
        overwrite=True
    )

    cursor.close()
    conn.close()

    if not success:
        raise Exception("Failed to load RAW_BIKE_RIDES")

    return f"Loaded {nrows} rows into RAW.RAW_BIKE_RIDES"


# DBT TRANSFORMATION ASSET
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
)
def bike_share_dbt_assets(context, dbt: DbtCliResource):
    """
    Runs full dbt build across STAGING → INTERMEDIATE → MART
    """
    yield from dbt.cli(["build"], context=context).stream()
