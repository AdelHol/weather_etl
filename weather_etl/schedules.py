from dagster import ScheduleDefinition
from weather_etl.jobs import weather_etl_job

weather_etl_schedule = ScheduleDefinition(
    job=weather_etl_job,
    cron_schedule="*/15 * * * *",  # každých 15 minut
)

