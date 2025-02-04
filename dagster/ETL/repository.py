from dagster import repository
from extract_job import etl_job
from extract_sensor import weather_api_sensor
from dagster import sensor, RunRequest
from S3_to_Redshift_job import s3_backup_sensor

# Repository to register the job
@repository
def my_repository():
    return [etl_job, weather_api_sensor, s3_backup_sensor]

