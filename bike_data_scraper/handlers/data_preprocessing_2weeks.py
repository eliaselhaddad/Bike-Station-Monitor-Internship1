from io import StringIO
import boto3
from loguru import logger
import pandas as pd


def get_data_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"])
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
    logger.info("Merging both columns")
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
        s3 = boto3.resource("s3")
        SingleBikes = df[df["stationId"].str.startswith("BIKE")]
        StationaryStations = df[~df["stationId"].str.startswith("BIKE")]

        csv_buffer_single_bikes = StringIO()
        SingleBikes.to_csv(csv_buffer_single_bikes, index=False)
        s3.Object(bucket, f"{path}/SingleBikes.csv").put(
            Body=csv_buffer_single_bikes.getvalue()
        )
        csv_buffer_single_bikes.close()

        csv_buffer_stationary_stations = StringIO()
        StationaryStations.to_csv(csv_buffer_stationary_stations, index=False)
        s3.Object(bucket, f"{path}/StationaryStations.csv").put(
            Body=csv_buffer_stationary_stations.getvalue()
        )
        csv_buffer_stationary_stations.close()

        logger.info("Successfully saved to S3.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def lambda_handler():
    bucket = "raw-data-weather-and-bikes"
    weather_key = "weather-09-29-10-13.csv"
    bikes_key = "bikes-09-29-10-13.csv"

    df = None

    weather_data = get_data_from_s3(bucket, weather_key)
    bikes_data = get_data_from_s3(bucket, bikes_key)

    if weather_data is not None and bikes_data is not None:
        weather_data, bikes_data = convert_time_to_more_features(
            weather_data, bikes_data
        )

        df = merge_both_columns({"weather": weather_data, "bikes": bikes_data})
    else:
        logger.warning("Oops! No data found")

    if df is not None:
        df = clean_unused_merge_columns(df)
        df = add_total_available_bikes_column(df)
        df = derive_weekend_feature(df)
        create_final_datasets_s3(df, bucket, "prep")
        logger.info("Done!")
    else:
        logger.warning("Something went wrong, data didnt process!")
