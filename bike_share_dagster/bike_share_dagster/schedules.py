from dagster import ScheduleDefinition
from .jobs import transformation_job

daily_transformation_schedule = ScheduleDefinition(
    job=transformation_job,
    cron_schedule="0 17 * * *",  # 5 PM daily
)
