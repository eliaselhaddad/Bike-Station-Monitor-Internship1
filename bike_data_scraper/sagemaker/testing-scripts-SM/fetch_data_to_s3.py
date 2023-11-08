import boto3
import csv
from datetime import datetime, timedelta
from io import StringIO
from boto3.dynamodb.conditions import Attr


# Initialize resources
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
table = dynamodb.Table("demo-dscrap-bike-data-table")
bucket_name = "danneftw-dscrap-bucket"


def fetch_and_save_data():
    # BEHÖVS FIXAS I RIKTIGA KODEN
    now = (datetime.now() - timedelta(days=1)).date()
    two_weeks_ago = (datetime.now() - timedelta(days=15)).date()

    # Fetch data from DynamoDB
    response = table.scan(
        Select="ALL_ATTRIBUTES",
        FilterExpression=Attr("timestamp").gte(two_weeks_ago.isoformat())
        & Attr("timestamp").lt(now.isoformat()),
    )

    items = response["Items"]

    while "LastEvaluatedKey" in response:
        response = table.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"],
            Select="ALL_ATTRIBUTES",
            FilterExpression=Attr("timestamp").gte(two_weeks_ago.isoformat())
            & Attr("timestamp").lt(now.isoformat()),
        )
        items.extend(response["Items"])

    if not items:
        print("No items found in DynamoDB for the last two weeks.")
        return

    # Convert to CSV
    output = StringIO()
    csv_writer = csv.DictWriter(output, fieldnames=items[0].keys())
    csv_writer.writeheader()
    for item in items:
        csv_writer.writerow(item)

    csv_data = output.getvalue()

    # Save to S3
    s3.put_object(
        Bucket=bucket_name, Key=f"two_weeks_data_{now.isoformat()}.csv", Body=csv_data
    )

    print(
        f"Data from the last two weeks saved to s3://{bucket_name}/two_weeks_data_{now.isoformat()}.csv"
    )


if __name__ == "__main__":
    fetch_and_save_data()
