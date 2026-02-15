from dagster import Definitions

from .assets import raw_bike_rides, bike_share_dbt_assets, dbt_resource
from .jobs import raw_ingestion_job, transformation_job, full_pipeline_job
from .sensors import raw_success_sensor
from .schedules import two_hour_schedule

defs = Definitions(
    assets=[raw_bike_rides, bike_share_dbt_assets],
    resources={
        "dbt": dbt_resource,
    },
    jobs=[
        raw_ingestion_job,
        transformation_job,
        full_pipeline_job,
    ],
    sensors=[raw_success_sensor],
    schedules=[two_hour_schedule],
)
