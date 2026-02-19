from dagster import define_asset_job, AssetSelection

ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=AssetSelection.groups("rds")
)

transformation_job = define_asset_job(
    name="transformation_job",
    selection=AssetSelection.groups("cds", "dds", "ids")
)
