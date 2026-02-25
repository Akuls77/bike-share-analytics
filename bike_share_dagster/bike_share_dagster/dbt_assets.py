from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator
from dagster import AssetExecutionContext
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt" / "bike_share_dbt"

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(Path.home() / ".dbt"),
)


# Custom translator to control group names
class LayerTranslator(DagsterDbtTranslator):
    def __init__(self, layer_name: str):
        super().__init__()
        self.layer_name = layer_name

    def get_group_name(self, dbt_resource_props):
        return self.layer_name


# CDS Layer
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:cds",
    dagster_dbt_translator=LayerTranslator("cds"),
)
def cds_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--select", "tag:cds"], context=context).stream()


# DDS Layer
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:dds",
    dagster_dbt_translator=LayerTranslator("dds"),
)
def dds_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--select", "tag:dds"], context=context).stream()


# IDS Layer
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:ids",
    dagster_dbt_translator=LayerTranslator("ids"),
)
def ids_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    #raise Exception("Intentional failure for sensor test")
    yield from dbt.cli(["build", "--select", "tag:ids"], context=context).stream()