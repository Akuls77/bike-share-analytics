from dagster import define_asset_job, AssetSelection

# RAW ingestion job
raw_ingestion_job = define_asset_job(
    name="raw_ingestion_job",
    selection=AssetSelection.keys("raw_bike_rides"),
)

# Transformation job (dbt only)
transformation_job = define_asset_job(
    name="transformation_job",
    selection=AssetSelection.groups("dbt"),
)

# Full pipeline job (for schedule)
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
)
