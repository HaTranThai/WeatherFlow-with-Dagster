import yaml
import requests
import json
import pandas as pd
import os
import sys
import argparse

from dagster import op, job, resource, Field, Output, In, Config
from dagster import sensor, RunRequest
from dagster import ScheduleDefinition, Definitions

from sqlalchemy import create_engine

from datetime import time

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = ['broker:29092']
KAFKA_TOPIC = 'weather_data'

@resource
def weather_api_resource(init_context):
    api_key = os.environ.get("WEATHER_API_KEY") 
    api_url = os.environ.get("WEATHER_API_URL") 

    return {"api_key": api_key, "api_url": api_url}

def configure_kafka(servers=KAFKA_BOOTSTRAP_SERVERS):
    settings = {
        'bootstrap.servers': ','.join(servers),
        'client.id': 'producer_instance'
    }
    return Producer(settings)

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Delivered message to {msg.topic()} [{msg.partition()}]')

@op(required_resource_keys={"weather_api"})
def extract_data(context):
    weather_api = context.resources.weather_api
    api_key = weather_api["api_key"]
    base_url = weather_api["api_url"]
    q = 'Hanoi'
    lang = 'vi'

    url = f"{base_url}?key={api_key}&q={q}&lang={lang}"

    response = requests.get(url)
    response.raise_for_status()

    api_response = response.json()
    context.log.info("Extracted data from API")
    return api_response

@op
def send_to_kafka(context, data: dict):
    producer = configure_kafka()

    producer.produce(KAFKA_TOPIC, json.dumps(data), callback=delivery_report)

    producer.flush()

    context.log.info("Send data to Kafka")

@job(resource_defs={"weather_api": weather_api_resource})
def extract_job():
    data = extract_data()
    send_to_kafka(data)

@sensor(target=extract_job)
def weather_api_sensor(context):
    weather_api = weather_api_resource(None)
    api_key = weather_api["api_key"]
    base_url = weather_api["api_url"]
    q = 'Hanoi'
    lang = 'vi'

    url = f"{base_url}?key={api_key}&q={q}&lang={lang}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        api_response = response.json()
        if "current" in api_response and "last_updated" in api_response["current"]:
            last_updated = api_response["current"]["last_updated"]
            if last_updated:
                context.log.info(f"New data available: {last_updated}")
                return RunRequest(run_key=str(last_updated), run_config={})
    except Exception as e:
        context.log.error(f"Error while checking API: {e}")
    return None

defs = Definitions(
    jobs=[extract_job],
    sensors=[weather_api_sensor],
    # schedules=[daily_schedule],
    resources={"weather_api": weather_api_resource},
)