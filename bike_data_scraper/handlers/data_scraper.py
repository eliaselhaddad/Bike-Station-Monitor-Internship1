import boto3
from datetime import datetime
import os

from bike_data_scraper.data_access_layer.dynamodb_handler import BikeDataDynamoDbHandler
from bike_data_scraper.http_client import HttpClient

bike_data_table_name = os.environ["BIKE_DATA_TABLE_NAME"]
http_client = HttpClient()
dynamodb_client = BikeDataDynamoDbHandler(bike_table_name=bike_data_table_name)

app_id = "d722487b-3b24-451c-87fe-db73219c9568"
url = f"https://data.goteborg.se/SelfServiceBicycleService/v2.0/Stations/{app_id}?getclosingperiods=true&latitude=57.7089&longitude=11.9746&radius=30000&format=json"


def lambda_handler(event, context):
    response = http_client.get_data(url)
    if response is None:
        raise Exception(f"Failed to get data from API: {url}")

    current_timestamp = datetime.now().isoformat()

    for station in response:
        station_id = station.get("Name") or station.get("StationId")
        item = {"stationId": station_id, "timestamp": current_timestamp}
        item.update({k: str(v) for k, v in station.items()})

        dynamodb_client.create_bike_data_item(item)

    print(f"Added {len(response)} stations to DynamoDB.")
