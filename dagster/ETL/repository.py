from dagster import repository
from extract_job import extract_job
from extract_sensor import weather_api_sensor
from dagster import sensor, RunRequest


# Repository to register the job
@repository
def my_repository():
    return [extract_job]

