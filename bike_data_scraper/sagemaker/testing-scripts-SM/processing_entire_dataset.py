import os
from loguru import logger
import boto3
from datetime import datetime
from io import StringIO
import boto3
import pandas as pd
from loguru import logger
import json


SOURCE_BUCKET = "danneftw-dscrap-bucket"
WEATHER_KEY = "weather_data_2_weeks.csv"
BIKES_KEY = "two_weeks_data_2023-11-01.csv"


def get_data_from_s3(bucket_name: str, key: str) -> pd.DataFrame:
    s3_client = boto3.client(
        "s3", region_name="eu-north-1"
    )  # Need to declare s3_client here
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(obj["Body"])
    if df.empty:
        logger.error(f"No data found for bucket {bucket_name} and key {key}")
        raise Exception("No data found in S3 bucket")
    return df


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

        # Check if the 'Time' column exists and rename it if 'timestamp' does not exist
        if "Time" in df.columns and "timestamp" not in df.columns:
            df.rename(columns={"Time": "timestamp"}, inplace=True)

        # Check if 'timestamp' column exists
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

            df["Year"] = df["timestamp"].dt.year
            df["Month"] = df["timestamp"].dt.month
            df["Day"] = df["timestamp"].dt.day
            df["Time_of_Day"] = df["timestamp"].dt.strftime("%H:%M:%S")
            df["Time_of_Day"] = pd.to_datetime(df["Time_of_Day"], format="%H:%M:%S")
            df["Hour"] = df["Time_of_Day"].dt.hour
            df["Minute"] = df["Time_of_Day"].dt.minute
        else:
            logger.warning(
                f"{name} data does not contain a 'timestamp' column. Skipping timestamp conversion."
            )

        df_dict[name] = df

    return tuple(df_dict.values())


def merge_both_datasets(data_dict: dict) -> pd.DataFrame:
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
        columns=["Time_of_Day_y", "Minute_y", "Time_of_Day_x", "timestamp_y"],
        axis=1,
        inplace=True,
    )
    df.rename(columns={"Minute_x": "Minute", "timestamp_x": "timestamp"}, inplace=True)
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


def get_min_and_max_dates_from_dataframe(df: pd.DataFrame) -> dict:
    min_date = df["timestamp"].min().strftime("%Y-%m-%d")
    max_date = df["timestamp"].max().strftime("%Y-%m-%d")
    return {"min_date": min_date, "max_date": max_date}


def save_dataframe_to_s3(
    df: pd.DataFrame,
    bucket_name: str,
    path_name: str,
    current_date: str,
    filename: str,
    sub_path: str,
):
    s3_resource = boto3.resource("s3")

    with StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        s3_resource.Object(
            bucket_name, f"{path_name}/{sub_path}/{current_date}/{filename}.csv"
        ).put(Body=csv_buffer.getvalue())


def create_final_datasets_s3(
    df: pd.DataFrame, bucket: str, path_name: str, sub_path: str
):
    try:
        SingleBikes = df[df["stationId"].str.startswith("BIKE")]
        StationaryStations = df[~df["stationId"].str.startswith("BIKE")]
        logger.info(SingleBikes.columns)
        current_date_singles = get_min_and_max_dates_from_dataframe(SingleBikes)
        current_date_stations = get_min_and_max_dates_from_dataframe(StationaryStations)

        save_dataframe_to_s3(
            df=SingleBikes,
            bucket_name=bucket,
            path_name=path_name,
            sub_path=f"{sub_path}/single_bikes",
            current_date=f"{current_date_singles['min_date']}-{current_date_singles['max_date']}",
            filename="SingleBikes",
        )
        save_dataframe_to_s3(
            df=StationaryStations,
            bucket_name=bucket,
            path_name=path_name,
            sub_path=f"{sub_path}/station_bikes",
            current_date=f"{current_date_stations['min_date']}-{current_date_stations['max_date']}",
            filename="StationaryStations",
        )

        logger.info(
            f"Successfully saved to Single and Stations bike data to S3 {bucket}/{path_name}/{sub_path}."
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e


def main():
    df = None
    try:
        weather_data = get_data_from_s3(SOURCE_BUCKET, WEATHER_KEY)
        bikes_data = get_data_from_s3(SOURCE_BUCKET, BIKES_KEY)

        if weather_data is None or bikes_data is None:
            raise ValueError("Missing data for weather or bikes")

        weather_data, bikes_data = convert_time_to_more_features(
            weather_data, bikes_data
        )
        df = merge_both_datasets({"weather": weather_data, "bikes": bikes_data})

        if df is None:
            raise ValueError("DataFrame is empty")

        df = clean_unused_merge_columns(df)
        df = add_total_available_bikes_column(df)
        df = derive_weekend_feature(df)

        path_name = "processed"
        sub_path = "two_weeks"
        create_final_datasets_s3(df, SOURCE_BUCKET, path_name, sub_path)
        logger.info("Data successfully processed.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e


if __name__ == "__main__":
    main()
