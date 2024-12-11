from dagster import op, job, resource, Field, Output, In, Config
from dagster import sensor, RunRequest
from dagster import ScheduleDefinition, Definitions

from extract_job import extract_job, weather_api_resource

import requests

@sensor(target=extract_job)
def weather_api_sensor(context):
    weather_api = weather_api_resource(None)
    api_key = weather_api["api_key"]
    base_url = weather_api["api_url"]

    params = {
        "id": 1559969,
        "appid": api_key,
        "lang": "vi"
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        api_response = response.json()
        last_updated = api_response["dt"]
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