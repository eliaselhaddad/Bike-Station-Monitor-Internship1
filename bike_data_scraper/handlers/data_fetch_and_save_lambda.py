import boto3
import csv
from datetime import datetime, timedelta
from io import StringIO
import os

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")


def lambda_handler(event, context):
    # Read from DynamoDB
    table_name = os.environ["BIKE_TABLE_NAME"]
    table = dynamodb.Table(table_name)

    # Calculate timestamp for two weeks ago
    two_weeks_ago = datetime.now() - timedelta(days=14)  # "from today and 14 days back"

    # kan testa att ta bort hh:mm:ss

    # filtering for records where timestamp is greater than the calculated 'two_weeks_ago'
    response = table.scan(
        FilterExpression="#ts > :two_weeks_ago",
        ExpressionAttributeNames={"#ts": "timestamp"},
        ExpressionAttributeValues={":two_weeks_ago": two_weeks_ago.isoformat()},
    )
    # checks if there are any items in the response
    items = response["Items"] if "Items" in response and response["Items"] else []

    # if no items are found
    if not items:
        return {"message": "No items found in DynamoDB for the last two weeks."}

    # Convert to CSV format
    csv_file = StringIO()  # behaves like a file but data is stored in memory
    csv_writer = csv.DictWriter(
        csv_file, fieldnames=items[0].keys()
    )  # assuming all items have the same keys
    csv_writer.writeheader()  # writes the header (column names)
    for item in items:  # loops through the items
        csv_writer.writerow(item)  # writes a row for each loop
    csv_data = csv_file.getvalue()
    csv_file.close()

    # Write to S3
    bucket_name = os.environ["S3_BUCKET_NAME"]
    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")  #
    s3_key = f"testdata_{timestamp_str}.csv"
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_data)

    return {
        "message": f"Data from the last two weeks saved to s3://{bucket_name}/{s3_key}"
    }
