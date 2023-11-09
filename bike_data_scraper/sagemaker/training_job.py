import argparse
import pandas as pd
import boto3
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from io import StringIO
import logging


def read_csv_from_s3(bucket_name, file_path):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
    return df


def upload_metrics_to_s3(bucket_name, metrics, file_path):
    s3 = boto3.client("s3")
    metrics_str = StringIO()
    pd.DataFrame([metrics]).to_csv(metrics_str, index=False)
    s3.put_object(Bucket=bucket_name, Key=file_path, Body=metrics_str.getvalue())
    logging.info(f"Metrics uploaded to S3 bucket sagemaker-eu-north-1-796717305864")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--bucket-name", type=str, required=True)
    parser.add_argument("--file-path", type=str, required=True)

    args = parser.parse_args()

    df = read_csv_from_s3(args.bucket_name, args.file_path)

    df["Visibility"] = df["Visibility"].fillna(
        23180
    )  # average visibility the week before missing visbility data (only missing for 29-31 august)

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

    X = df.drop("TotalAvailableBikes", axis=1)
    y = df["TotalAvailableBikes"]

    # 0.5% test data only for metrics
    xtrain, xtest, ytrain, ytest = train_test_split(
        X, y, test_size=0.005, random_state=42
    )

    model = RandomForestRegressor()
    model.fit(xtrain, ytrain)

    # metrics to upload to s3
    ypred = model.predict(xtest)
    mae = mean_absolute_error(ytest, ypred)
    mse = mean_squared_error(ytest, ypred)

    metrics = {
        "MAE": mae,
        "MSE": mse,
        "R2": r2_score(ytest, ypred),
    }

    upload_metrics_to_s3(
        "sagemaker-eu-north-1-796717305864", metrics, "metrics/metrics-full-dataset.csv"
    )

    joblib.dump(model, "/opt/ml/model/model.joblib")
