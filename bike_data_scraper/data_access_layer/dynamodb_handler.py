from datetime import timedelta, datetime

import boto3
from boto3.dynamodb.conditions import Key, Attr, Between, BeginsWith


class DynamoDbHandler:
    def __init__(self, bike_table_name):
        self.bike_table_name = bike_table_name
        self.dynamodb = boto3.resource("dynamodb")
        self.bike_table = self.dynamodb.Table(self.bike_table_name)

    def get_bike_data_last_two_weeks_from_datetime(self, starting_date: datetime):
        from_date = (starting_date + timedelta(days=1)).date()
        two_weeks_ago = (starting_date - timedelta(days=14)).date()

        response = self.bike_table.scan(
            Select="ALL_ATTRIBUTES",
            FilterExpression=Attr("timestamp").gte("2023-09-25")
            & Attr("timestamp").lt("2023-10-19"),
        )

        print(f"response: {response['Items']}")
        print(f"response length: {len(response['Items'])}")
        if not response["Items"]:
            return {"message": "No items found in DynamoDB for the last two weeks."}

        bikes = response["Items"]

        while "LastEvaluatedKey" in response:
            response = self.bike_table.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            bikes.extend(response["Items"])

        return bikes

    def get_all_bike_data(self):
        response = self.bike_table.scan()
        bikes = response["Items"]

        while "LastEvaluatedKey" in response:
            response = self.bike_table.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            bikes.extend(response["Items"])

        return bikes
