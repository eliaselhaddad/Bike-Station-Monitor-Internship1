import boto3
import csv
from datetime import datetime, timedelta
from io import StringIO
import os

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
table_name = os.environ["BIKE_TABLE_NAME"]
table = dynamodb.Table(table_name)
bucket_name = os.environ["S3_BUCKET_NAME"]
stage_name = os.environ.get("STAGE_NAME")
service_short_name = os.environ.get("SERVICE_SHORT_NAME")


def getBikeDataLastTwoWeeks():
    one_day_ago = datetime.now() - datetime.timedelta(days=1)
    one_day_ago_date = one_day_ago.date()
    two_weeks_ago = one_day_ago_date - datetime.timedelta(days=14)

    response = table.scan(
        FilterExpression="begins_with(#ts, :two_weeks_ago) OR begins_with(#ts, :one_day_ago)",
        ExpressionAttributeNames={"#ts": "timestamp"},
        ExpressionAttributeValues={
            ":two_weeks_ago": two_weeks_ago.isoformat(),
            ":one_day_ago": one_day_ago_date.isoformat(),
        },
    )

    items = response["Items"] if "Items" in response and response["Items"] else []
    if not items:
        raise Exception("No items found in the last two weeks (from yesterday)")

    return items


def ConvertToCSV(items):
    csv_file = StringIO()
    csv_writer = csv.DictWriter(csv_file, fieldnames=items[0].keys())
    csv_writer.writeheader()

    for item in items:
        csv_writer.writerow(item)

    csv_data = csv_file.getvalue()
    csv_file.close()

    return csv_data


def PutInS3Bucket(csv_data):
    one_day_ago = datetime.now() - datetime.timedelta(days=1)
    s3_key = f"bikes_two_weeks_{one_day_ago.isoformat()}.csv"
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_data)

    return s3_key


def lambda_handler(event, context):
    items = getBikeDataLastTwoWeeks()
    csv_data = ConvertToCSV(items)
    s3_key = PutInS3Bucket(csv_data)

    return {
        "message": f"Data from the last two weeks saved to s3://{bucket_name}/{s3_key}"
    }
