from dagster import define_asset_job
from .dbt_assets import cds_dbt_assets, dds_dbt_assets, ids_dbt_assets

cds_job = define_asset_job(
    name="cds_job",
    selection=[cds_dbt_assets],
)

dds_job = define_asset_job(
    name="dds_job",
    selection=[dds_dbt_assets],
)

ids_job = define_asset_job(
    name="ids_job",
    selection=[ids_dbt_assets],
)