from dagster import sensor, RunRequest
from .jobs import transformation_job

@sensor(job=transformation_job)
def raw_success_sensor(context):
    records = context.instance.get_run_records(limit=5)

    for record in records:
        if (
            record.dagster_run.job_name == "raw_ingestion_job"
            and record.dagster_run.status.value == "SUCCESS"
        ):
            return RunRequest(
                run_key=None,
                run_config={}
            )

    return None
