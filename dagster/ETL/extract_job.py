from dagster import op, job, resource, Field, Output, In, Config
from dagster import sensor, RunRequest
from dagster import ScheduleDefinition, Definitions

import os
from extract_op import extract_data, send_to_kafka
from weather.AirPollutionData import AirPollutionData
from weather.CurrentData import CurrentData

@resource
def weather_api_resource(init_context):
    api_key = os.environ.get("WEATHER_API_KEY") 
    api_url = os.environ.get("CURRENT_API_URL") 

    return {"api_key": api_key, "api_url": api_url}

@resource
def air_pollution_data_resource(init_context):
    api_key = os.environ.get("WEATHER_API_KEY") 
    api_url = os.environ.get("AIR_POLLUTION_API_URL") 
    kafka_ip = os.environ.get("KAFKA_ADDRESS")
    kafka_server = [f"{kafka_ip}:9092"]

    return AirPollutionData(api_key, api_url, kafka_server)

@resource
def current_data_resource(init_context):
    api_key = os.environ.get("WEATHER_API_KEY") 
    api_url = os.environ.get("CURRENT_API_URL") 
    kafka_ip = os.environ.get("KAFKA_ADDRESS")
    kafka_server = [f"{kafka_ip}:9092"]

    return CurrentData(api_key, api_url, kafka_server)

@job(resource_defs={"air_pollution_data": air_pollution_data_resource, 
                    "current_data": current_data_resource,
                    "weather_api": weather_api_resource})
def etl_job():
    messages = extract_data()
    send_to_kafka(messages)