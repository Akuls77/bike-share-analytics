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
            COPY INTO RDS.RAW_BIKE_RIDES
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
                FROM @RDS.BIKE_STAGE t
            )
        """)

        context.log.info("Initial ingestion completed successfully.")

    finally:
        cursor.close()