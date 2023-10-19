import boto3
from datetime import datetime, timedelta
import os

from bike_data_scraper.s3_client.s3_handler import S3Handler
from bike_data_scraper.data_access_layer.dynamodb_handler import DynamoDbHandler
from bike_data_scraper.csv_client.csv_handler import CSVHandler

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
table_name = os.environ["BIKE_TABLE_NAME"]
table = dynamodb.Table(table_name)
bucket_name = os.environ["S3_BUCKET_NAME"]
stage_name = os.environ.get("STAGE_NAME")
service_short_name = os.environ.get("SERVICE_SHORT_NAME")


def lambda_handler(event, context):
    items = DynamoDbHandler(table_name).get_bike_data_last_two_weeks()

    columns = items[0].keys()
    csv_data = CSVHandler.convert_to_csv(columns, items)

    one_day_ago = datetime.now() - timedelta(days=1)
    s3_key = f"bikes_two_weeks_{one_day_ago.isoformat()}.csv"
    S3Handler().put_in_s3_bucket(
        filename=s3_key, data_to_save=csv_data, bucket_name=bucket_name
    )

    return {
        "message": f"Data from the last two weeks saved to s3://{bucket_name}/{s3_key}"
    }
