import http.client
import urllib.parse
import boto3
import json
import csv
from io import StringIO
import pandas as pd
import datetime as dt

API_ENDPOINT = "api.open-meteo.com"
PATH = "/v1/forecast"
bucket_name = "danneftw-dscrap-bucket"
bike_key_name = "two_weeks_data_2023-11-01.csv"
weather_key_name = "weather_data_2_weeks.csv"

s3 = boto3.client("s3")


def main():
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
    save_data_to_s3(csv_str)


def calculate_dates():
    # Fetch bike dataset from S3
    obj = s3.get_object(Bucket=bucket_name, Key=bike_key_name)
    df = pd.read_csv(obj["Body"])

    # Get date range from dataset
    start_date = pd.to_datetime(df["timestamp"]).min().date()
    end_date = pd.to_datetime(df["timestamp"]).max().date()
    return start_date, end_date


def fetch_weather_data(url: str, api_endpoint: str) -> dict:
    conn = http.client.HTTPSConnection(api_endpoint)
    conn.request("GET", url)
    response = conn.getresponse()

    if response.status == 200:
        try:
            response_data = response.read()
            weather_data = json.loads(response_data)
            return weather_data
        finally:
            conn.close()
    else:
        try:
            error_message = response.read().decode()
            raise Exception(f"Failed to retrieve weather data: {error_message}")
        finally:
            conn.close()


def convert_to_csv(weather_data: dict) -> str:
    hourly_data = weather_data["hourly"]
    old_fieldnames = [
        "time",
        "temperature_2m",
        "relativehumidity_2m",
        "windspeed_10m",
        "precipitation",
        "visibility",
        "snowfall",
    ]
    new_fieldnames = [
        "Time",
        "Temperature",
        "Humidity",
        "Wind_Speed",
        "Precipitation",
        "Visibility",
        "Snowfall",
    ]
    rename_map = {
        "time": "Time",
        "temperature_2m": "Temperature",
        "relativehumidity_2m": "Humidity",
        "windspeed_10m": "Wind_Speed",
        "precipitation": "Precipitation",
        "visibility": "Visibility",
        "snowfall": "Snowfall",
    }

    csv_data = []
    for i in range(len(hourly_data["time"])):
        row = {
            rename_map.get(field, field): hourly_data[field][i]
            for field in old_fieldnames
        }
        csv_data.append(row)

    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=new_fieldnames)
    writer.writeheader()
    for row in csv_data:
        writer.writerow(row)

    return csv_buffer.getvalue()


def save_data_to_s3(csv_str: str):
    s3 = boto3.client("s3")
    s3.put_object(Body=csv_str, Bucket=bucket_name, Key=weather_key_name)


if __name__ == "__main__":
    main()
