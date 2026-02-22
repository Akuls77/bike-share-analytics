from dagster import ScheduleDefinition
from .jobs import cds_job

daily_cds_schedule = ScheduleDefinition(
    job=cds_job,
    cron_schedule="0 */2 * * *",
)