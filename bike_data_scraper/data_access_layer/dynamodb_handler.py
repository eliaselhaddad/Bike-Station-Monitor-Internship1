from datetime import timedelta, datetime

import boto3
from boto3.dynamodb.conditions import Key


class DynamodbHandler:
    def __init__(self, bike_table_name):
        self.bike_table_name = bike_table_name
        self.dynamodb = boto3.resource("dynamodb")
        self.device_table = self.dynamodb.Table(self.bike_table_name)

    # OBS EJ TESTAD, INGEN ANING HUR/OM DETTA FUNKAR UTAN ÄR ETT EXEMPEL
    def get_bike_data_last_two_weeks(self):
        one_day_ago = datetime.now() - timedelta(days=1)
        one_day_ago_date = one_day_ago.date()
        two_weeks_ago = one_day_ago_date - timedelta(days=14)
        filter_expression = {
            "FilterExpression": (
                "begins_with(#timestamp, :val1) OR begins_with(#timestamp, :val2)"
            ),
            "ExpressionAttributeNames": {"#timestamp": "timestamp"},
            "ExpressionAttributeValues": {
                ":val1": {"S": one_day_ago_date},
                ":val2": {"S": two_weeks_ago},
            },
        }

        response = self.bike_table_name.scan(
            FilterExpression=filter_expression,
            ExpressionAttributeNames={"#ts": "timestamp"},
            ExpressionAttributeValues={":two_weeks_ago": two_weeks_ago.isoformat()},
        )
        if not response["Items"]:
            return {"message": "No items found in DynamoDB for the last two weeks."}

        bikes = response["Items"]

        while "LastEvaluatedKey" in response:
            response = self.bike_table_name.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            bikes.extend(response["Items"])

        return bikes
