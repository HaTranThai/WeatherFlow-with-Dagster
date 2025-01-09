from dagster import repository
from extract_job import etl_job
from extract_sensor import weather_api_sensor
from dagster import sensor, RunRequest


# Repository to register the job
@repository
def my_repository():
    return [etl_job, weather_api_sensor]

