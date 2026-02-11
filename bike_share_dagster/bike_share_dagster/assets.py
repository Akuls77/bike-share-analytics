from dagster import asset
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

@asset
def raw_bike_rides():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    cursor = conn.cursor()
    cursor.execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}")
    cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
    cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')}")

    csv_path = r"C:\Bike-Share-Analytics\data\raw_csv\NYC-BikeShare-2015-2017-combined.csv"
    df = pd.read_csv(csv_path, index_col=0)

    df.columns = [c.lower().replace(" ", "_") for c in df.columns]


    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name="RAW_BIKE_RIDES",
        auto_create_table=True,
        overwrite=True
    )

    conn.close()
    return f"Loaded {nrows} rows"
