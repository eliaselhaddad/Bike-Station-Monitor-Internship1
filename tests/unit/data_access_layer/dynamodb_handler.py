from datetime import datetime, timedelta, date
import boto3
import pytest
import moto
from freezegun import freeze_time

from bike_data_scraper.data_access_layer.dynamodb_handler import BikeDataDynamoDbHandler

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


@freeze_time("2023-10-20 00:00:00")
def test_get_bike_data_last_two_weeks(data_table_with_transactions):
    # ARRANGE
    dynamodb_handler = BikeDataDynamoDbHandler(TABLE_NAME)
    # ACT
    response = dynamodb_handler.get_bike_data_last_two_weeks_from_datetime(
        datetime.now()
    )
    # ASSERT
    assert response[0]["AvailableBikes"] == "3"
    assert len(response) == 4


@freeze_time("2023-09-26 00:00:00")
def test_get_bike_data_last_two_weeks(data_table_with_transactions):
    # ARRANGE
    dynamodb_handler = BikeDataDynamoDbHandler(TABLE_NAME)
    # ACT
    response = dynamodb_handler.get_bike_data_last_two_weeks_from_datetime(
        datetime.now()
    )
    # ASSERT
    assert response[0]["AvailableBikes"] == "1"
    assert len(response) == 1
