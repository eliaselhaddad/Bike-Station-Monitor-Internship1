import boto3
import csv
from datetime import datetime, timedelta
from io import StringIO
import os

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")


def lambda_handler(event, context):
    table_name = os.environ["BIKE_TABLE_NAME"]
    table = dynamodb.Table(table_name)

    two_weeks_ago = datetime.now() - timedelta(days=14)

    response = table.scan(
        FilterExpression="#ts > :two_weeks_ago",
        ExpressionAttributeNames={"#ts": "timestamp"},
        ExpressionAttributeValues={":two_weeks_ago": two_weeks_ago.isoformat()},
    )

    items = response["Items"] if "Items" in response and response["Items"] else []

    if not items:
        return {"message": "No items found in DynamoDB for the last two weeks."}

    csv_file = StringIO()
    csv_writer = csv.DictWriter(csv_file, fieldnames=items[0].keys())
    csv_writer.writeheader()
    for item in items:
        csv_writer.writerow(item)
    csv_data = csv_file.getvalue()
    csv_file.close()

    bucket_name = "bikedatalake"
    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
    s3_key = f"bike_data_{timestamp_str}.csv"
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_data)

    return {
        "message": f"Data from the last two weeks saved to s3://{bucket_name}/{s3_key}"
    }
