from dagster import sensor, RunRequest, DagsterRunStatus
from .jobs import cds_job, dds_job, ids_job


@sensor(job=dds_job)
def cds_success_sensor(context):

    runs = context.instance.get_runs(
        filters={"job_name": "cds_job"},
        limit=1,
    )

    if not runs:
        return

    latest_run = runs[0]

    if latest_run.status == DagsterRunStatus.SUCCESS:
        run_key = f"dds_after_{latest_run.run_id}"

        if context.cursor == run_key:
            return

        context.update_cursor(run_key)
        yield RunRequest(run_key=run_key)


@sensor(job=ids_job)
def dds_success_sensor(context):

    runs = context.instance.get_runs(
        filters={"job_name": "dds_job"},
        limit=1,
    )

    if not runs:
        return

    latest_run = runs[0]

    if latest_run.status == DagsterRunStatus.SUCCESS:
        run_key = f"ids_after_{latest_run.run_id}"

        if context.cursor == run_key:
            return

        context.update_cursor(run_key)
        yield RunRequest(run_key=run_key)