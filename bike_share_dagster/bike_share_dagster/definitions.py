from dagster import Definitions, define_asset_job, ScheduleDefinition
from .assets import raw_bike_rides
from dagster import sensor, RunRequest, DefaultSensorStatus

ingestion_job = define_asset_job(
    name = "bike_ingestion_job",
    selection = "raw_bike_rides",
)

ingestion_schedule = ScheduleDefinition(
    job = ingestion_job,
    cron_schedule = "0 * * * *"
)

@sensor(job = ingestion_job, default_status = DefaultSensorStatus.RUNNING)
def ingestion_success_sensor(context):
    for run in context.instance.get_runs(limit=5):
        if run.status.value == "SUCCESS":
            yield RunRequest(run_key=None)
            return

defs = Definitions(
    assets=[raw_bike_rides],
    jobs = [ingestion_job],
    schedules = [ingestion_schedule],
    #sensors = [ingestion_success_sensor],
)
