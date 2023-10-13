import os
import http.client
import urllib.parse
import boto3
import json
import csv
from io import StringIO
from typing import Any, Dict
import datetime as dt


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    # calculate start and end date
    today = dt.date.today()
    start_date = today - dt.timedelta(days=14)
    end_date = today
    # Define the API endpoint and parameters
    api_endpoint = "api.open-meteo.com"
    path = "/v1/forecast"
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m,precipitation,visibility,snowfall",
    }

    # Format parameters for inclusion in URL
    params_str = urllib.parse.urlencode(params)
    url = f"{path}?{params_str}"

    # Create an HTTPS connection and make a GET request
    conn = http.client.HTTPSConnection(api_endpoint)
    conn.request("GET", url)
    response = conn.getresponse()

    # Check for a successful response
    if response.status == 200:
        response_data = response.read()
        weather_data = json.loads(response_data)
        print(weather_data)  # Print out response

        # Check if 'hourly' key exists in weather_data
        if "hourly" not in weather_data:
            error_message = "'hourly' key missing in weather data"
            print(error_message)
            conn.close()  # Close the connection
            return {
                "statusCode": 400,
                "body": error_message,
            }

        # Convert JSON to CSV
        hourly_data = weather_data["hourly"]
        fieldnames = [
            "time",
            "temperature_2m",
            "relativehumidity_2m",
            "windspeed_10m",
            "precipitation",
            "visibility",
            "snowfall",
        ]
        csv_data = []
        for i in range(len(hourly_data["time"])):
            row = {field: hourly_data[field][i] for field in fieldnames}
            csv_data.append(row)

        # Prepare CSV data string
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_data:
            writer.writerow(row)

        # Get CSV data as string
        csv_str = csv_buffer.getvalue()

        # Save CSV data to S3 bucket
        s3 = boto3.client("s3")
        object_key = f"weather-data-{end_date.strftime('%Y-%m-%d')}.csv"
        # object_key = "weather-data.csv"
        weather_bucket_name = os.environ.get("S3_BUCKET_NAME")
        s3.put_object(Bucket=weather_bucket_name, Key=object_key, Body=csv_str)

        # Close the connection
        conn.close()

        return {"statusCode": 200, "body": "Weather data saved successfully!"}

    else:
        error_message = response.read().decode()
        print(f"Failed to retrieve weather data: {error_message}")
        conn.close()  # Close the connection
        return {
            "statusCode": response.status,
            "body": f"Failed to retrieve weather data: {error_message}",
        }
