import yaml
from dagster import op, job, resource, Field, Output, In, Config
from sqlalchemy import create_engine
# import weatherapi
# from weatherapi.rest import ApiException
import pandas as pd
from dagster import sensor, RunRequest
from dagster import ScheduleDefinition, Definitions
from datetime import time
import requests
import json

def load_config(config_path):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

# Load cấu hình
config = load_config("config.yaml")

# Resource: Kết nối cơ sở dữ liệu PostgreSQL
@resource
def postgres_connection(context):
    db_url = config["postgres"]["db_url"]  # URL kết nối đến PostgreSQL
    engine = create_engine(db_url)  # Kết nối PostgreSQL bằng chuỗi kết nối
    return engine

# Step 1: Thu thập dữ liệu từ API
@op
def fetch_data_from_api(context):
    # Cấu hình API key
    api_key = config["fetch_data_from_api"]["api_key"]
    base_url = config["fetch_data_from_api"]["api_url"]  # Endpoint của WeatherAPI
    q = 'Hanoi'  # Địa điểm
    lang = 'vi'  # Ngôn ngữ (tiếng Việt)

    # Tạo URL yêu cầu
    url = f"{base_url}?key={api_key}&q={q}&lang={lang}"

    # Gửi yêu cầu GET tới API
    response = requests.get(url)
    response.raise_for_status()  # Kiểm tra lỗi HTTP
    
    # Phân tích dữ liệu trả về (JSON)
    api_response = response.json()

    return api_response

# Step 2: Làm sạch dữ liệu
@op
def clean_data(context, raw_data: dict) -> pd.DataFrame:
    context.log.info("Cleaning data...")
    # Chuyển dữ liệu thành DataFrame
    weather = raw_data.get("location", {})
    current = raw_data.get("current", {})

    # Tạo DataFrame từ các trường quan trọng
    df = pd.DataFrame([{
        "location_name": weather.get("name"),
        "region": weather.get("region"),
        "country": weather.get("country"),
        "temperature_c": current.get("temp_c"),
        "humidity": current.get("humidity"),
        "wind_kph": current.get("wind_kph"),
        "last_updated": current.get("last_updated")
    }])
    return df

# Step 3: Lưu dữ liệu vào cơ sở dữ liệu
@op(required_resource_keys={"db"})
def save_to_db(context, clean_data: pd.DataFrame):
    context.log.info("Saving data to database...")
    engine = context.resources.db
    # Lưu vào bảng weather_data
    clean_data.to_sql("weather_data", engine, if_exists="append", index=False)
    context.log.info("Data saved successfully!")

# Pipeline: Kết nối các bước
# Define the ETL job
@job(resource_defs={"db": postgres_connection})
def etl_job():
    raw_data = fetch_data_from_api()
    cleaned_data = clean_data(raw_data)
    save_to_db(cleaned_data)


@sensor(target=etl_job)
def weather_api_sensor(context):
    try:
        # Cấu hình API key
        api_key = config["fetch_data_from_api"]["api_key"]
        base_url = config["fetch_data_from_api"]["api_url"]  # Endpoint của WeatherAPI
        q = 'Hanoi'  # Địa điểm
        lang = 'vi'  # Ngôn ngữ (tiếng Việt)

        # Tạo URL yêu cầu
        url = f"{base_url}?key={api_key}&q={q}&lang={lang}"

        # Gửi yêu cầu GET tới API
        response = requests.get(url)
        response.raise_for_status()  # Kiểm tra lỗi HTTP
        
        # Phân tích dữ liệu trả về (JSON)
        api_response = response.json()

        # Kiểm tra dữ liệu trả về có hợp lệ không
        if "current" in api_response and "last_updated" in api_response["current"]:
            last_updated = api_response["current"]["last_updated"]

            # Kiểm tra nếu thời gian cập nhật là dữ liệu mới
            if last_updated:  # Thay bằng logic kiểm tra cụ thể
                context.log.info(f"New data available: {last_updated}")
                return RunRequest(run_key=str(last_updated), run_config=())
        else:
            context.log.warning("Invalid API response: Missing 'current' or 'last_updated'")
    except Exception as e:
        context.log.error(f"Error while checking API: {e}")

    return None


# Định nghĩa lịch trình (Cron expression chạy mỗi phút)
daily_schedule = ScheduleDefinition(
    job=etl_job,  # Job cần chạy
    cron_schedule="* * * * *",  # Cron expression để chạy mỗi phút
    name="daily_etl_schedule",  # Tên của lịch trình
)

defs = Definitions(
    jobs=[etl_job],  # Job cần chạy
    sensors=[weather_api_sensor],
    # schedules=[daily_schedule],  # Lịch trình cho job này
    resources={"db": postgres_connection},  # Định nghĩa resources
)