import requests
import boto3
from datetime import datetime
import os

ddb = boto3.client("dynamodb", region_name="eu-north-1")
bike_data_table_name = os.environ["BIKE_DATA_TABLE_NAME"]


def lambda_handler(event, context):
    app_id = "d722487b-3b24-451c-87fe-db73219c9568"
    url = f"https://data.goteborg.se/SelfServiceBicycleService/v2.0/Stations/{app_id}?getclosingperiods=true&latitude=57.7089&longitude=11.9746&radius=30000&format=json"

    try:
        response = requests.get(url)
        stations = response.json()

        current_timestamp = datetime.now().isoformat()

        for station in stations:
            StationId = station.get("Name") or station.get("StationId")
            params = {
                "TableName": bike_data_table_name,
                "Item": {
                    "stationId": {"S": StationId},
                    "timestamp": {"S": current_timestamp},
                    **{k: {"S": str(v)} for k, v in station.items()},
                },
            }

            ddb.put_item(**params)

        print(f"Added {len(stations)} stations to DynamoDB.")
    except Exception as e:
        raise e
