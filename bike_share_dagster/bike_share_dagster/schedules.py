from dagster import ScheduleDefinition
from .jobs import full_pipeline_job

two_hour_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 */2 * * *",
)
