from dagster import Definitions
from .assets import load_raw_bike_rides
from .resources import snowflake_connection

defs = Definitions(
    assets=[load_raw_bike_rides],
    resources={
        "snowflake_connection": snowflake_connection,
    },
)
