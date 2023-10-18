import os
from datetime import datetime
from io import StringIO
import sys

import boto3
import pandas as pd
from loguru import logger


from bike_data_scraper.s3_client.s3_handler import S3Handler

WEATHER_KEY = os.environ["WEATHER_KEY"]
BIKES_KEY = os.environ["BIKES_KEY"]
DESTINATION_BUCKET = os.environ["S3_DESTINATION_BUCKET"]
SOURCE_BUCKET = os.environ["S3_SOURCE_BUCKET"]

CURRENT_DATE = datetime.now().strftime("%d-%m-%Y")


s3_handler = S3Handler()


def lambda_handler(event, context):
    df = None

    try:
        weather_data = s3_handler.get_data_from_s3(SOURCE_BUCKET, WEATHER_KEY)
        bikes_data = s3_handler.get_data_from_s3(SOURCE_BUCKET, BIKES_KEY)

        if weather_data is None or bikes_data is None:
            raise ValueError("Missing data for weather or bikes")

        weather_data, bikes_data = convert_time_to_more_features(
            weather_data, bikes_data
        )

        df = merge_both_columns({"weather": weather_data, "bikes": bikes_data})
    except Exception as e:
        logger.error(f"Oops! No data found in dataframe for merging. Error: {e}")
        raise e

    try:
        if df is None:
            raise ValueError("DataFrame is empty")

        df = clean_unused_merge_columns(df)
        df = add_total_available_bikes_column(df)
        df = derive_weekend_feature(df)
        create_final_datasets_s3(df, DESTINATION_BUCKET, "processed")
        logger.info(
            print(
                f"Done! Data successfully processed and saved in https://s3.console.aws.amazon.com/s3/buckets/{DESTINATION_BUCKET}/processed/{CURRENT_DATE}/"
            )
        )
    except Exception as e:
        logger.error(f"Something went wrong, data didn't process! Error: {e}")
        raise e


def convert_time_to_more_features(
    weather_data: pd.DataFrame, bikes_data: pd.DataFrame
) -> tuple:
    logger.info("Converting time to more features")

    data_frames = {"weather": weather_data, "bikes": bikes_data}
    df_dict = {}

    for name, df in data_frames.items():
        logger.info(f"Processing {name} data")

        if df is None:
            logger.warning(f"{name} data be empty, so we skip.")
            continue

        if "Time" in df.columns and "timestamp" not in df.columns:
            df.rename(columns={"Time": "timestamp"}, inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        df["Year"] = df["timestamp"].dt.year
        df["Month"] = df["timestamp"].dt.month
        df["Day"] = df["timestamp"].dt.day
        df["Time_of_Day"] = df["timestamp"].dt.strftime("%H:%M:%S")
        df["Time_of_Day"] = pd.to_datetime(df["Time_of_Day"], format="%H:%M:%S")
        df["Hour"] = df["Time_of_Day"].dt.hour
        df["Minute"] = df["Time_of_Day"].dt.minute

        df_dict[name] = df

    return tuple(df_dict.values())


def merge_both_columns(data_dict: dict) -> pd.DataFrame:
    logger.info("Merging weather and bikes datasets on columns: Year, Month, Day, Hour")
    merge_columns = ["Year", "Month", "Day", "Hour"]

    weather_data = data_dict.get("weather")
    bikes_data = data_dict.get("bikes")
    df = pd.merge(
        bikes_data,
        weather_data,
        how="left",
        left_on=merge_columns,
        right_on=merge_columns,
    )
    return df


def clean_unused_merge_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Removing unused merge columns")
    df.drop(
        columns=["Time_of_Day_y", "Minute_y", "Time_of_Day_x"], axis=1, inplace=True
    )
    df.rename(columns={"Minute_x": "Minute"}, inplace=False)
    return df


def derive_weekend_feature(df: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["IsWeekend"] = df["timestamp"].dt.dayofweek >= 5
        df["IsWeekend"] = df["IsWeekend"].astype(int)
        return df
    else:
        logger.warning(
            "There is no timestamp column in the DataFrame, so we can't derive the weekend feature"
        )
        return df


def add_total_available_bikes_column(df: pd.DataFrame) -> pd.DataFrame:
    if "BikeIds" in df.columns:
        df["TotalAvailableBikes"] = df["BikeIds"].apply(lambda x: len(eval(x)))
        logger.info("Added new column: TotalAvailableBikes")
    else:
        logger.warning(
            "The DataFrame doesn't have a 'BikeIds' column. Skipping this step."
        )
    return df


def create_final_datasets_s3(df: pd.DataFrame, bucket: str, path: str):
    try:
        SingleBikes = df[df["stationId"].str.startswith("BIKE")]
        StationaryStations = df[~df["stationId"].str.startswith("BIKE")]

        s3_handler.save_dataframe_to_s3(SingleBikes, bucket, path, "SingleBikes")
        s3_handler.save_dataframe_to_s3(
            StationaryStations, bucket, path, "StationaryStations"
        )

        logger.info(
            f"Successfully saved to Single and Stations bike data to S3 {bucket}/{path}."
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e
