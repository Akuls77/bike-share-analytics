import os
from dagster import sensor, RunRequest
from .jobs import ingestion_job, transformation_job
from .config import DataPaths

LAST_MODIFIED = None

@sensor(job=ingestion_job)
def file_change_sensor(context):

    global LAST_MODIFIED

    file_path = DataPaths.RAW_CSV_PATH

    if not os.path.exists(file_path):
        return

    current_modified = os.path.getmtime(file_path)

    if LAST_MODIFIED is None:
        LAST_MODIFIED = current_modified
        return

    if current_modified > LAST_MODIFIED:
        LAST_MODIFIED = current_modified
        yield RunRequest(run_key=f"ingestion_{current_modified}")

@sensor(job=transformation_job)
def ingestion_completion_sensor(context):

    records = context.instance.get_run_records(limit=1)

    if not records:
        return

    last_run = records[0].dagster_run

    if last_run.job_name == "ingestion_job" and last_run.status.value == "SUCCESS":
        yield RunRequest(run_key=f"transform_after_{last_run.run_id}")
