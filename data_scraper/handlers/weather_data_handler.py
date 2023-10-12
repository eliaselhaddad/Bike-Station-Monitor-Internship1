import os
import http.client
import urllib.parse
import boto3
import json
import csv
from io import StringIO
from typing import Any, Dict


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    # Define the API endpoint and parameters
    api_endpoint = "api.open-meteo.com"
    path = "/v1/forecast"
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "current": "temperature_2m,windspeed_10m",
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m",
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

        # Convert JSON to CSV
        hourly_data = weather_data["hourly"]
        fieldnames = ["time", "temperature_2m", "relativehumidity_2m", "windspeed_10m"]
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
        bucket_name = "raw-data-weather-and-bikes"
        object_key = "weather-data.csv"

        s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_str)

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
