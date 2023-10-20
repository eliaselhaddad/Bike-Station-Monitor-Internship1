from datetime import timedelta, datetime

import boto3
from boto3.dynamodb.conditions import Attr


class BikeDataDynamoDbHandler:
    def __init__(self, bike_table_name):
        self.bike_table_name = bike_table_name
        self.dynamodb = boto3.resource("dynamodb")
        self.bike_table = self.dynamodb.Table(self.bike_table_name)

    def get_bike_data_last_two_weeks_from_datetime(self, starting_date: datetime):
        from_date = (starting_date + timedelta(days=1)).date()
        two_weeks_ago = (starting_date - timedelta(days=14)).date()

        response = self.bike_table.scan(
            Select="ALL_ATTRIBUTES",
            FilterExpression=Attr("timestamp").gte(f"{two_weeks_ago}")
            & Attr("timestamp").lt(f"{from_date}"),
        )

        if not response["Items"]:
            return {"message": "No items found in DynamoDB for the last two weeks."}

        bikes = response["Items"]

        while "LastEvaluatedKey" in response:
            response = self.bike_table.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            bikes.extend(response["Items"])

        return bikes

    def create_bike_data_item(self, item: dict):
        self.bike_table.put_item(Item=item)

    @staticmethod
    def create_dynamodb_item(pk: str, sk: str, item: dict) -> dict:
        return {
            "Item": {"stationId": {"S": pk}, "timestamp": {"S": sk}, **item.items()}
        }
