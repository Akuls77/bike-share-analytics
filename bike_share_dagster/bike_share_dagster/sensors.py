from dagster import run_status_sensor, RunRequest, DagsterRunStatus
from .jobs import cds_job, dds_job, ids_job
from dagster import sensor, RunsFilter
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
import os

# Email Sending Function
def send_failure_email(job_name: str, run_id: str):

    alert_email = os.getenv("ALERT_EMAIL")
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", 587))

    if not all([alert_email, smtp_user, smtp_password]):
        raise Exception("SMTP environment variables are not properly configured.")

    subject = "🚨 Bike Share Pipeline Failure Alert"

    body = f"""
DAGSTER PIPELINE FAILURE ALERT

Pipeline Job   : {job_name}
Run ID         : {run_id}
Triggered At   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Environment    : Development
Orchestration  : Dagster
Transformation : dbt
Warehouse      : Snowflake

--------------------------------------------------
Action Required:
Please log into Dagster UI immediately
and inspect the failing step logs.

This is an automated monitoring alert.
"""

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = alert_email

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, alert_email, msg.as_string())


# Failure Monitoring Sensor
@sensor(name="pipeline_failure_sensor")
def pipeline_failure_sensor(context):

    # Fetch latest run across all jobs
    runs = context.instance.get_runs(limit=1)

    if not runs:
        context.log.info("No runs found.")
        return

    latest_run = runs[0]

    context.log.info(f"Latest run job: {latest_run.job_name}")
    context.log.info(f"Latest run status: {latest_run.status}")

    if latest_run.status == DagsterRunStatus.FAILURE:

        run_key = f"failure_{latest_run.run_id}"

        # Prevent duplicate alerts
        if context.cursor == run_key:
            context.log.info("Failure already alerted.")
            return

        context.log.info("Failure detected. Sending alert email...")

        send_failure_email(
            job_name=latest_run.job_name,
            run_id=latest_run.run_id,
        )

        context.update_cursor(run_key)



# Trigger DDS when CDS succeeds
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS
)
def cds_success_sensor(context):

    if context.dagster_run.job_name != "cds_job":
        return

    return RunRequest(
        run_key=f"dds_after_{context.dagster_run.run_id}"
    )


# Trigger IDS when DDS succeeds
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS
)
def dds_success_sensor(context):

    if context.dagster_run.job_name != "dds_job":
        return

    return RunRequest(
        run_key=f"ids_after_{context.dagster_run.run_id}"
    )
