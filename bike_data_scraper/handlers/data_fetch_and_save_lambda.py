import boto3
from datetime import datetime, timedelta
import os

from bike_data_scraper.s3_client.s3_handler import S3Handler
from bike_data_scraper.data_access_layer.dynamodb_handler import BikeDataDynamoDbHandler
from bike_data_scraper.csv_client.csv_handler import UniversalCSVConverter

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
table_name = os.environ["BIKE_TABLE_NAME"]
table = dynamodb.Table(table_name)
bucket_name = os.environ["S3_BUCKET_NAME"]
stage_name = os.environ.get("STAGE_NAME")
service_short_name = os.environ.get("SERVICE_SHORT_NAME")


def lambda_handler(event, context):
    items = BikeDataDynamoDbHandler(
        table_name
    ).get_bike_data_last_two_weeks_from_datetime()

    columns = items[0].keys()
    csv_data = UniversalCSVConverter(columns=columns, data=items).to_csv()

    one_day_ago = datetime.now() - timedelta(days=1)
    s3_key = f"bikes_two_weeks_{one_day_ago.isoformat()}.csv"
    S3Handler(bucket_name).put_in_s3_bucket(filename=s3_key, data_to_save=csv_data)

    return {
        "message": f"Data from the last two weeks saved to s3://{bucket_name}/{s3_key}"
    }
