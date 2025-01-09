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

@op(required_resource_keys={"air_pollution_data", "current_data"})
def extract_data(context):
    air_pollution = context.resources.air_pollution_data
    current = context.resources.current_data

    file_path = "/app/data/filtered_vn_1.json"

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    messages = []
    for item in data:
        city_id = item["id"]
        lat = item["coord"]["lat"]
        lon = item["coord"]["lon"]

        current_data = current.extract_current_weather(city_id)
        air_pollution_data = air_pollution.extract_air_pollution(lat, lon)

        messages.append({
            "city_id": city_id,
            "current_data": current_data,
            "air_pollution_data": air_pollution_data
        })

    context.log.info(f"Extracted {len(messages)} messages")
    return messages

@op(required_resource_keys={"air_pollution_data", "current_data"})
def send_to_kafka(context, messages: list):
    current = context.resources.current_data
    air_pollution = context.resources.air_pollution_data

    for message in messages:
        current.send_to_kafka("Current_data", message["current_data"])
        air_pollution.send_to_kafka("Air_pollution_data", message["air_pollution_data"])

    context.log.info(f"Sent {len(messages)} messages to Kafka successfully")
