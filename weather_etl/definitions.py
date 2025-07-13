from dagster import Definitions, load_assets_from_modules
import weather_etl.assets as assets
from weather_etl.resources import pg_conn_resource
from weather_etl.jobs import weather_etl_job
from weather_etl.schedules import weather_etl_schedule

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[weather_etl_job],
    schedules=[weather_etl_schedule],
    resources={"pg_conn": pg_conn_resource},
)
