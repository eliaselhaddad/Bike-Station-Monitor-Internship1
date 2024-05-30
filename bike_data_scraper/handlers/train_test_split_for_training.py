import os
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd
from loguru import logger
import json
from sklearn.model_selection import train_test_split
from bike_data_scraper.s3_client.s3_handler import S3Handler
from bike_data_scraper.libs.functions import get_min_and_max_dates_from_dataframe


SOURCE_BUCKET = os.environ.get("TRAINING_SOURCE_BUCKET")
DESTINATION_BUCKET = os.environ.get("TRAINING_DESTINATION_BUCKET")

STATION_BIKE_DATA_KEY = os.environ.get("STATION_BIKE_DATA")
SINGLE_BIKE_DATA_KEY = os.environ.get("SINGLE_BIKE_DATA")

S3_CLIENT = boto3.client("s3")
S3_RESOURCE = boto3.resource("s3")
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")

s3_handler = S3Handler()


def lambda_handler(event, context):
    try:
        logger.info("Getting data from S3")
        station_bikes_data = s3_handler.get_data_from_s3(
            SOURCE_BUCKET, STATION_BIKE_DATA_KEY
        )
        # single_bikes_data = s3_handler.get_data_from_s3(
        #     SOURCE_BUCKET, SINGLE_BIKE_DATA_KEY
        # )
        current_date = get_min_and_max_dates_from_dataframe(station_bikes_data)
        date = f"{current_date['min_date']}-{current_date['max_date']}"

        target, features = split_dataset_into_target_and_features(station_bikes_data)
        X_train, X_test, y_train, y_test = split_dataset_into_train_and_test_sets(
            target, features
        )
        save_results(
            df=X_train,
            date_sub_folder=date,
            sub_path="train",
            filename="X_train",
        )
        save_results(
            df=X_test,
            date_sub_folder=date,
            sub_path="test",
            filename="X_test",
        )
        save_results(
            df=y_train,
            date_sub_folder=date,
            sub_path="train",
            filename="y_train",
        )
        save_results(
            df=y_test,
            date_sub_folder=date,
            sub_path="test",
            filename="y_test",
        )
        save_results(
            df=target,
            date_sub_folder=date,
            sub_path="single_bikes",
            filename="final_xtrain",
        )
        save_results(
            df=features,
            date_sub_folder=date,
            sub_path="station_bikes",
            filename="final_ytrain",
        )
    except Exception as e:
        logger.error(f"Error splitting dataset into train and test sets: {e}")
        raise e


def split_dataset_into_target_and_features(df: pd.DataFrame) -> tuple:
    try:
        logger.info("Splitting dataset into target and features")
        target = df["availableBikes"]
        features = df.drop("availableBikes", axis=1)
        return target, features
    except Exception as e:
        logger.error(f"Error splitting dataset into target and features: {e}")
        raise e


def split_dataset_into_train_and_test_sets(
    target: pd.DataFrame, features: pd.DataFrame
) -> tuple:
    try:
        logger.info("Splitting dataset into train and test sets")
        X_train, X_test, y_train, y_test = train_test_split(
            features, target, test_size=0.2, random_state=42
        )
        return X_train, X_test, y_train, y_test
    except Exception as e:
        logger.error(f"Error splitting dataset into train and test sets: {e}")
        raise e


def save_results(df: pd.DataFrame, date_sub_folder: str, sub_path: str, filename: str):
    s3_handler.save_dataframe_to_s3(
        data=df,
        bucket_name=DESTINATION_BUCKET,
        path_name="training",
        sub_path=sub_path,
        current_date=date_sub_folder,
        filename=filename,
    )
