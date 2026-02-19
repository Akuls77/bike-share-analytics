from dagster_dbt import DbtCliResource, load_assets_from_dbt_project
from dagster import AssetExecutionContext
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt" / "bike_share_dbt"

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
)

# RDS Layer
rds_dbt_assets = load_assets_from_dbt_project(
    project_dir=str(DBT_PROJECT_DIR),
    select="tag:rds",
    group_name="rds",
)

# CDS Layer
cds_dbt_assets = load_assets_from_dbt_project(
    project_dir=str(DBT_PROJECT_DIR),
    select="tag:cds",
    group_name="cds",
)

# DDS Layer
dds_dbt_assets = load_assets_from_dbt_project(
    project_dir=str(DBT_PROJECT_DIR),
    select="tag:dds",
    group_name="dds",
)

# IDS Layer
ids_dbt_assets = load_assets_from_dbt_project(
    project_dir=str(DBT_PROJECT_DIR),
    select="tag:ids",
    group_name="ids",
)
