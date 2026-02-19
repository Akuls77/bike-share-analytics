from dagster_dbt import dbt_assets, DbtCliResource
from dagster import AssetExecutionContext
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt" / "bike_share_dbt"

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
)

# RDS Layer
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:rds",
)
def rds_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--select", "tag:rds"], context=context).stream()


# CDS Layer
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:cds",
)
def cds_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--select", "tag:cds"], context=context).stream()


# DDS Layer
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:dds",
)
def dds_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--select", "tag:dds"], context=context).stream()


# IDS Layer
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:ids",
)
def ids_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--select", "tag:ids"], context=context).stream()
