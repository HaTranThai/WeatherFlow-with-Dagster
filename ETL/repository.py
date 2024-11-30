from dagster import repository
from etl_job import etl_job
from dagster import sensor, RunRequest


# Repository to register the job
@repository
def my_repository():
    return [etl_job]

