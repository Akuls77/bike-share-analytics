from dagster import Definitions
from .assets import load_raw_bike_rides
from .dbt_assets import cds_dbt_assets, dds_dbt_assets, ids_dbt_assets, dbt_resource
from .resources import snowflake_connection
from .jobs import cds_job, dds_job, ids_job
from .schedules import daily_cds_schedule
from .sensors import cds_success_sensor, dds_success_sensor, pipeline_failure_sensor

defs = Definitions(
    assets=[
        load_raw_bike_rides,
        cds_dbt_assets,
        dds_dbt_assets,
        ids_dbt_assets,
    ],
    resources={
        "snowflake_connection": snowflake_connection,
        "dbt": dbt_resource,
    },
    jobs=[cds_job, dds_job, ids_job],
    schedules=[daily_cds_schedule],
    sensors=[
        cds_success_sensor,
        dds_success_sensor,
        pipeline_failure_sensor,
    ],
)