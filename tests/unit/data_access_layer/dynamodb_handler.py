import boto3
import pytest
import moto
from datetime import datetime

from bike_data_scraper.data_access_layer.dynamodb_handler import DynamodbHandler

TABLE_NAME = "demo-dscrap-bike-data-table"


@pytest.fixture()
def data_table():
    with moto.mock_dynamodb():
        client = boto3.client("dynamodb")
        client.create_table(
            AttributeDefinitions=[
                {"AttributeName": "stationId", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"},
            ],
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "stationId", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        yield TABLE_NAME


@pytest.fixture
def data_table_with_transactions(data_table):
    """Creates transactions for a client with a total of 9"""

    table = boto3.resource("dynamodb").Table(TABLE_NAME)

    txs = [
        {
            "stationId": "station1",
            "timestamp": "2023-09-25T14:50:34.453006",
            "AvailableBikes": "1",
        },
        {
            "stationId": "station2",
            "timestamp": "2023-10-05T15:00:34.581457",
            "AvailableBikes": "2",
        },
        {
            "stationId": "station3",
            "timestamp": "2023-10-12T15:10:35.982498",
            "AvailableBikes": "3",
        },
        {
            "stationId": "station4",
            "timestamp": "2023-10-18T15:10:35.982498",
            "AvailableBikes": "4",
        },
        {
            "stationId": "station5",
            "timestamp": "2023-10-18T23:59:59.999999",
            "AvailableBikes": "5",
        },
        {
            "stationId": "station6",
            "timestamp": "2023-10-19T00:00:00.000000",
            "AvailableBikes": "6",
        },
    ]

    for tx in txs:
        table.put_item(Item=tx)


def test_get_all_bike_data(data_table_with_transactions):
    dynamodb_handler = DynamodbHandler(TABLE_NAME)
    response = dynamodb_handler.get_all_bike_data()
    assert response[0]["AvailableBikes"] == "1"
    assert len(response) == 4


def test_get_bike_data_last_two_weeks(data_table_with_transactions):
    dynamodb_handler = DynamodbHandler(TABLE_NAME)
    print(datetime.now())
    response = dynamodb_handler.get_bike_data_last_two_weeks()
    # assert response[0]["AvailableBikes"] == "2"
    assert len(response) == 3
