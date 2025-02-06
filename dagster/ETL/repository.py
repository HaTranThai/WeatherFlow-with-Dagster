from dagster import repository
from extract_job import etl_job
from extract_sensor import weather_api_sensor
from dagster import sensor, RunRequest
from Project.dagster.ETL.S3_to_Redshift.S3_to_Redshift_job_Current import S3_Current
from Project.dagster.ETL.S3_to_Redshift.S3_to_Redshift_job_Pollution import S3_Pollution

# Repository to register the job
@repository
def my_repository():
    return [etl_job, weather_api_sensor, S3_Current, S3_Pollution]

