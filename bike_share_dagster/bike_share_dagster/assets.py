from dagster import asset
from .config import DataPaths


@asset(
    group_name="rds",
    required_resource_keys={"snowflake_connection"},
)
def load_raw_bike_rides(context):

    conn = context.resources.snowflake_connection
    cursor = conn.cursor()

    try:
        context.log.info("Checking if RAW_BIKE_RIDES table already exists...")

        cursor.execute("""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'RDS'
            AND TABLE_NAME = 'RAW_BIKE_RIDES'
        """)

        table_exists = cursor.fetchone()[0]

        if table_exists:
            context.log.info("RAW_BIKE_RIDES already exists. Skipping ingestion.")
            return

        context.log.info("Creating file format...")

        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT RDS.BIKE_CSV_FORMAT
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
        """)

        context.log.info("Creating stage...")

        cursor.execute("""
            CREATE OR REPLACE STAGE RDS.BIKE_STAGE
            FILE_FORMAT = RDS.BIKE_CSV_FORMAT
        """)

        file_path = DataPaths.RAW_CSV_PATH.replace("\\", "/")

        context.log.info("Uploading file to stage...")

        cursor.execute(f"""
            PUT file://{file_path}
            @RDS.BIKE_STAGE
            AUTO_COMPRESS=TRUE
        """)

        context.log.info("Creating RAW table...")

        cursor.execute("""
            CREATE TABLE RDS.RAW_BIKE_RIDES (
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

        context.log.info("Copying data into RAW table...")

        cursor.execute("""
            COPY INTO RDS.RAW_BIKE_RIDES (
                trip_duration,
                start_time,
                stop_time,
                start_station_id,
                start_station_name,
                start_station_latitude,
                start_station_longitude,
                end_station_id,
                end_station_name,
                end_station_latitude,
                end_station_longitude,
                bike_id,
                user_type,
                birth_year,
                gender,
                trip_duration_in_min
            )
            FROM @RDS.BIKE_STAGE
            FILE_FORMAT = (FORMAT_NAME = RDS.BIKE_CSV_FORMAT)
        """)

        context.log.info("Initial ingestion completed successfully.")

    finally:
        cursor.close()
