import os
import http.client
import urllib.parse
import boto3
import json
import csv
from io import StringIO
from typing import Any, Dict
import datetime as dt

from bike_data_scraper.s3_client.s3_handler import S3Handler

# Define constants
API_ENDPOINT = "api.open-meteo.com"
PATH = "/v1/forecast"
s3_handler = S3Handler()
bucket_name = os.environ["S3_BUCKET_NAME"]


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    start_date, end_date = calculate_dates()
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m,precipitation,visibility,snowfall",
    }
    params_str = urllib.parse.urlencode(params)
    url = f"{PATH}?{params_str}"

    weather_data = fetch_weather_data(url, API_ENDPOINT)

    if weather_data is None:
        raise Exception("Weather data could not be fetched from API!")

    csv_str = convert_to_csv(weather_data)
    response = S3Handler().save_data_as_string_to_csv(
        csv_str=csv_str, end_date=end_date.strftime("%Y-%m-%d"), bucket_name=bucket_name
    )

    return {
        "statusCode": 200,
        "body": "Weather data saved successfully!",
        "s3_info": response,
    }


def calculate_dates():
    today = dt.date.today()
    start_date = today - dt.timedelta(days=14)
    end_date = today
    return start_date, end_date


def fetch_weather_data(url: str, api_endpoint: str) -> dict:
    conn = http.client.HTTPSConnection(api_endpoint)
    conn.request("GET", url)
    response = conn.getresponse()

    if response.status == 200:
        try:
            response_data = response.read()
            print(f"Response data: {response_data}")
            weather_data = json.loads(response_data)
            return weather_data
        finally:
            conn.close()
    else:
        try:
            error_message = response.read().decode()
            print(f"Failed to retrieve weather data: {error_message}")
            raise Exception(f"Failed to retrieve weather data: {error_message}")
        finally:
            conn.close()


def convert_to_csv(weather_data: dict) -> str:
    hourly_data = weather_data["hourly"]
    fieldnames = ["time", "temperature_2m", "relativehumidity_2m", "windspeed_10m"]
    csv_data = []
    for i in range(len(hourly_data["time"])):
        row = {field: hourly_data[field][i] for field in fieldnames}
        csv_data.append(row)

    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in csv_data:
        writer.writerow(row)

    return csv_buffer.getvalue()
