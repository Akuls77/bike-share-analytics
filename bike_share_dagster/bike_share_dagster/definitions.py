from dagster import Definitions
from .assets import load_raw_bike_rides
from .dbt_assets import (
    rds_dbt_assets,
    cds_dbt_assets,
    dds_dbt_assets,
    ids_dbt_assets,
    dbt_resource
)
from .resources import snowflake_connection
from .jobs import ingestion_job, transformation_job, full_pipeline_job
from .schedules import weekly_pipeline_schedule
from .sensors import file_change_sensor

defs = Definitions(
    assets=[
        load_raw_bike_rides,
        *rds_dbt_assets,
        *cds_dbt_assets,
        *dds_dbt_assets,
        *ids_dbt_assets,
    ],
    resources={
        "snowflake_connection": snowflake_connection,
        "dbt": dbt_resource,
    },
    jobs=[
        ingestion_job,
        transformation_job,
        full_pipeline_job,
    ],
    schedules=[weekly_pipeline_schedule],
    sensors=[file_change_sensor],
)
