from dagster import Definitions, define_asset_job, ScheduleDefinition
from .assets import raw_bike_rides, bike_share_dbt_assets, dbt_resource
from dagster import sensor, RunRequest, DefaultSensorStatus
from dagster import AssetSelection, RunsFilter

ingestion_job = define_asset_job(
    name="bike_ingestion_job",
    selection="raw_bike_rides",
)

full_pipeline_job = define_asset_job(
    name="bike_full_pipeline",
    selection=AssetSelection.all() - AssetSelection.keys("raw_bike_rides"),
)

ingestion_schedule = ScheduleDefinition(
    job=ingestion_job,
    cron_schedule="0 * * * *",
)

@sensor(job=full_pipeline_job, default_status=DefaultSensorStatus.RUNNING)
def ingestion_success_sensor(context):
    runs = context.instance.get_runs(
        filters=RunsFilter(job_name="bike_ingestion_job"),
        limit=1,
    )

    if not runs:
        return

    latest_run = runs[0]

    if latest_run.status.value == "SUCCESS":
        yield RunRequest(run_key=latest_run.run_id)

defs = Definitions(
    assets=[raw_bike_rides, bike_share_dbt_assets],
    jobs=[ingestion_job, full_pipeline_job],
    schedules=[ingestion_schedule],
    sensors=[ingestion_success_sensor],
    resources={"dbt": dbt_resource},
)