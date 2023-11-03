import boto3
import pandas as pd
from sklearn.model_selection import train_test_split
from io import StringIO

SOURCE_BUCKET = "processed-bike-data"
SOURCE_KEY = "StationaryStations.csv"
DEST_BUCKET = "sagemaker-eu-north-1-796717305864"
DEST_PREFIX = "sagemaker/sklearncontainer/"

COLUMNS_TO_KEEP = [
    "IsOpen",
    "Long",
    "Lat",
    "Year",
    "Month",
    "Day",
    "Hour",
    "Temperature",
    "Humidity",
    "Wind_Speed",
    "Precipitation",
    "Visibility",
    "Snowfall",
    "IsWeekend",
    "TotalAvailableBikes",
]


def fetch_from_s3(bucket_name, file_key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))


def save_to_s3(dataframe, bucket_name, file_key):
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    s3 = boto3.resource("s3")
    s3.Object(bucket_name, file_key).put(Body=csv_buffer.getvalue())


def main():
    df = fetch_from_s3(SOURCE_BUCKET, SOURCE_KEY)

    df.dropna(inplace=True)

    df = df[COLUMNS_TO_KEEP]

    X = df.drop("TotalAvailableBikes", axis=1)
    y = df["TotalAvailableBikes"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    save_to_s3(X_train, DEST_BUCKET, DEST_PREFIX + "xtrain2.csv")
    save_to_s3(X_test, DEST_BUCKET, DEST_PREFIX + "xtest2.csv")
    save_to_s3(y_train, DEST_BUCKET, DEST_PREFIX + "ytrain2.csv")
    save_to_s3(y_test, DEST_BUCKET, DEST_PREFIX + "ytest2.csv")

    print("Done!")


if __name__ == "__main__":
    main()
