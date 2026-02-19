import snowflake.connector
from dagster import resource
from .config import SnowflakeConfig

@resource
def snowflake_connection(_):

    conn = snowflake.connector.connect(
        account=SnowflakeConfig.ACCOUNT,
        user=SnowflakeConfig.USER,
        password=SnowflakeConfig.PASSWORD,
        role=SnowflakeConfig.ROLE,
        warehouse=SnowflakeConfig.WAREHOUSE,
        database=SnowflakeConfig.DATABASE,
        schema=SnowflakeConfig.SCHEMA,
    )

    try:
        yield conn
    finally:
        conn.close()
