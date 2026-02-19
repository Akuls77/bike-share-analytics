from dagster import asset
from .config import DataPaths

@asset(group_name="rds", required_resource_keys={"snowflake_connection"})
def load_raw_bike_rides(context):

    conn = context.resources.snowflake_connection
    cursor = conn.cursor()

    try:
        context.log.info("Creating file format...")

        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT bike_csv_format
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
        """)

        context.log.info("Creating stage...")

        cursor.execute("""
            CREATE OR REPLACE STAGE bike_stage
            FILE_FORMAT = bike_csv_format
        """)

        context.log.info("Uploading file to stage...")

        file_path = DataPaths.RAW_CSV_PATH.replace("\\", "/")

        cursor.execute(f"""
            PUT file://{file_path}
            @bike_stage
            AUTO_COMPRESS=TRUE
        """)

        context.log.info("Creating raw table...")

        cursor.execute("""
            CREATE OR REPLACE TABLE RAW_BIKE_RIDES (
                trip_duration INTEGER,
                start_time TIMESTAMP,
                stop_time TIMESTAMP,
                start_station_id INTEGER,
                start_station_name STRING,
                start_station_latitude FLOAT,
                start_station_longitude FLOAT,
                end_station_id INTEGER,
                end_station_name STRING,
                end_station_latitude FLOAT,
                end_station_longitude FLOAT,
                bike_id INTEGER,
                user_type STRING,
                birth_year FLOAT,
                gender INTEGER,
                trip_duration_in_min INTEGER
            )
        """)

        context.log.info("Copying data into table...")

        cursor.execute("""
            COPY INTO RAW_BIKE_RIDES
            FROM (
                SELECT
                    t.$2,
                    t.$3,
                    t.$4,
                    t.$5,
                    t.$6,
                    t.$7,
                    t.$8,
                    t.$9,
                    t.$10,
                    t.$11,
                    t.$12,
                    t.$13,
                    t.$14,
                    t.$15,
                    t.$16,
                    t.$17
                FROM @bike_stage t
            )
        """)

        context.log.info("Data loaded successfully.")

    finally:
        cursor.close()
