import argparse
import pandas as pd
import boto3
from sklearn.ensemble import RandomForestRegressor
import joblib
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO)


# Function to read CSV file from S3
def read_csv_from_s3(bucket_name, file_path):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
    return df


# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Expect a single argument: the full dataset file path in S3
    parser.add_argument("--bucket-name", type=str, required=True)
    parser.add_argument("--file-path", type=str, required=True)

    args = parser.parse_args()

    # Read the dataset from S3
    df = read_csv_from_s3(args.bucket_name, args.file_path)

    # Preprocessing steps
    df["Visibility"] = df["Visibility"].fillna(0)

    COLUMNS_TO_KEEP = [
        "IsOpen",
        "Long",
        "Lat",
        "Year",
        "Month",
        "Day",
        "Hour",
        "Minute",
        "Temperature",
        "Humidity",
        "Windspeed",
        "Precipitation",
        "Visibility",
        "Snowfall",
        "IsWeekend",
        "TotalAvailableBikes",
    ]

    df = df[COLUMNS_TO_KEEP]

    # Splitting the data into features and target
    X = df.drop("TotalAvailableBikes", axis=1)
    y = df["TotalAvailableBikes"]

    # Train the model
    model = RandomForestRegressor()
    model.fit(X, y)

    # Save the trained model
    joblib.dump(model, "/opt/ml/model/model.joblib")
    logging.info("Model saved to /opt/ml/model/model.joblib")
