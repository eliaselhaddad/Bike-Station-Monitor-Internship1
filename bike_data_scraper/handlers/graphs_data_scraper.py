from io import StringIO
import os
import boto3
from datetime import datetime
import json
from loguru import logger
import pandas as pd
from bike_data_scraper.s3_client.s3_handler import S3Handler
from bike_data_scraper.libs.functions import get_min_and_max_dates_from_dataframe

SOURCE_BUCKET = os.environ.get("GRAPHS_SOURCE_BUCKET")
DESTINATION_BUCKET = os.environ.get("GRAPHS_DESTINATION_BUCKET")

SINGLE_BIKE_DATA_KEY = os.environ.get("SINGLE_BIKE_DATA")
STATION_BIKE_DATA_KEY = os.environ.get("STATION_BIKE_DATA")

S3_CLIENT = boto3.client("s3")
S3_RESOURCE = boto3.resource("s3")
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")

s3_handler = S3Handler()


def get_weather_data(bucket: str, key: str) -> pd.DataFrame:
    logger.info("Getting weather and bike data from S3")
    weather_data = S3_CLIENT.get_object(
        Bucket=bucket,
        Key=key,
    )
    weather_data = pd.read_csv(weather_data["Body"])
    return weather_data


def number_of_bikes_available(df: pd.DataFrame, item: str) -> object:
    logger.info("Calculating number of bikes available")
    number_of_unique_bikes_in_dataset = df["stationId"].nunique()
    unique_bike_object = f"{item}: {number_of_unique_bikes_in_dataset}"
    return unique_bike_object


def warmest_temperature(df: pd.DataFrame, dataframe_name: str) -> object:
    logger.info("Calculating warmest temperature")
    warmest_temperature = df["Temperature"].max()
    warmest_temperature_object = f"{dataframe_name}: {warmest_temperature}°C"
    return warmest_temperature_object


def coldest_temperature(df: pd.DataFrame, dataframe_name: str) -> object:
    logger.info("Calculating coldest temperature")
    coldest_temperature = df["Temperature"].min()
    coldest_temperature_object = f"{dataframe_name}: {coldest_temperature}°C"
    return coldest_temperature_object


def coldest_day(df: pd.DataFrame, dataframe_name: str) -> object:
    logger.info("Calculating coldest day")
    coldest_day = df["timestamp"].loc[df["Temperature"].idxmin()]
    coldest_day_object = f"{dataframe_name}: {coldest_day}"
    return coldest_day_object


def warmest_day(df: pd.DataFrame, dataframe_name: str) -> object:
    logger.info("Calculating warmest day")
    warmest_day = df["timestamp"].loc[df["Temperature"].idxmax()]
    warmest_day_object = f"{dataframe_name}: {warmest_day}"
    return warmest_day_object


def day_with_most_used_bikes(df: pd.DataFrame, dataframe_name: str) -> object:
    logger.info("Calculating day with most used bikes")
    day_with_most_used_bikes = df["timestamp"].value_counts().idxmax()
    day_with_most_used_bikes_object = f"{dataframe_name}: {day_with_most_used_bikes}"
    return day_with_most_used_bikes_object


def correlation_matrix_plot(df: pd.DataFrame, dataframe_name: str) -> object:
    logger.info("Calculating correlation matrix plot")
    corr_columns = [
        "AvailableBikes",
        "Distance",
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
    ]
    correlation_matrix = df[corr_columns].corr()
    correlation_matrix.to_csv(f"/tmp/{dataframe_name}_correlation_matrix.csv")
    return correlation_matrix


def weekend_vs_weekday_plot(df: pd.DataFrame, dataframe_name: str) -> object:
    logger.info("Calculating weekend vs weekday plot")
    average_number_of_bikes = (
        df.groupby("IsWeekend")["AvailableBikes"].mean().reset_index()
    )
    average_number_of_bikes["IsWeekend"] = average_number_of_bikes["IsWeekend"].replace(
        {0: "Weekdays", 1: "Weekend"}
    )
    average_number_of_bikes.to_csv(
        f"/tmp/{dataframe_name}_WeekendVsWeekdaysSingles.csv", index=False
    )
    return average_number_of_bikes


def weather_over_timestamp_plot(df: pd.DataFrame, dataframe_name: str) -> object:
    logger.info("Calculating weather over timestamp plot")
    grouped_df = (
        df.resample("H", on="timestamp")
        .agg(
            {
                "Humidity": "mean",
                "Wind_Speed": "mean",
                "Temperature": "mean",
                "TotalAvailableBikes": "sum",
            }
        )
        .reset_index()
    )
    grouped_df.to_csv(f"/tmp/{dataframe_name}_WeatherOverTimeStations.csv", index=False)
    return grouped_df


# def calculate_metrics(bike_data):
#     metrics = {}
#     metrics['number_of_bikes_available'] = number_of_bikes_available(bike_data, )
def process_station_data():
    try:
        station_bikes_data = get_weather_data(SOURCE_BUCKET, STATION_BIKE_DATA_KEY)
        current_date = get_min_and_max_dates_from_dataframe(station_bikes_data)

        if station_bikes_data.empty:
            raise Exception("No station data found in weather or bike buckets")

        number_of_bikes_available_stations = number_of_bikes_available(
            station_bikes_data, "number_of_bikes_available"
        )
        warmest_temperature_stations = warmest_temperature(
            station_bikes_data, "warmest_temperature"
        )
        coldest_temperature_stations = coldest_temperature(
            station_bikes_data, "coldest_temperature"
        )
        coldest_day_stations = coldest_day(station_bikes_data, "coldest_day")
        warmest_day_stations = warmest_day(station_bikes_data, "warmest_day")
        day_with_most_used_bikes_stations = day_with_most_used_bikes(
            station_bikes_data, "day_with_most_used_bikes"
        )
        correlation_matrix = correlation_matrix_plot(
            station_bikes_data, "correlation_matrix"
        )
        weekend_vs_weekday = weekend_vs_weekday_plot(
            station_bikes_data, "weekend_vs_weekday"
        )
        weather_over_timestamp = weather_over_timestamp_plot(
            station_bikes_data, "weather_over_timestamp"
        )

        results = {}
        results[
            "number_of_bikes_available_stations"
        ] = number_of_bikes_available_stations
        results["warmest_temperature_stations"] = warmest_temperature_stations
        results["coldest_temperature_stations"] = coldest_temperature_stations
        results["coldest_day_stations"] = coldest_day_stations
        results["warmest_day_stations"] = warmest_day_stations
        results["day_with_most_used_bikes_stations"] = day_with_most_used_bikes_stations
        results["correlation_matrix"] = correlation_matrix
        results["weekend_vs_weekday"] = weekend_vs_weekday
        results["weather_over_timestamp"] = weather_over_timestamp

        s3_handler.save_data_as_json(
            data=results,
            bucket_name=DESTINATION_BUCKET,
            path_name="graphs_data",
            sub_path="station_bikes",
            current_date=f"{current_date['min_date']}-{current_date['max_date']}",
        )

    except Exception as e:
        logger.error(f"Error: {e}")
        raise e


def process_single_data():
    try:
        single_bikes_data = get_weather_data(SOURCE_BUCKET, SINGLE_BIKE_DATA_KEY)
        current_date = get_min_and_max_dates_from_dataframe(single_bikes_data)
        if single_bikes_data.empty:
            raise Exception("No single data found in weather or bike buckets")

        number_of_bikes_available_singles = number_of_bikes_available(
            single_bikes_data, "number_of_bikes_available"
        )
        warmest_temperature_singles = warmest_temperature(
            single_bikes_data, "warmest_temperature"
        )
        coldest_temperature_singles = coldest_temperature(
            single_bikes_data, "coldest_temperature"
        )
        coldest_day_singles = coldest_day(single_bikes_data, "coldest_day")
        warmest_day_singles = warmest_day(single_bikes_data, "warmest_day")
        day_with_most_used_bikes_singles = day_with_most_used_bikes(
            single_bikes_data, "day_with_most_used_bikes"
        )
        correlation_matrix = correlation_matrix_plot(
            single_bikes_data, "correlation_matrix"
        )
        weekend_vs_weekday = weekend_vs_weekday_plot(
            single_bikes_data, "weekend_vs_weekday"
        )
        weather_over_timestamp = weather_over_timestamp_plot(
            single_bikes_data, "weather_over_timestamp"
        )

        results = {}
        results["number_of_bikes_available_singles"] = number_of_bikes_available_singles
        results["warmest_temperature_singles"] = warmest_temperature_singles
        results["coldest_temperature_singles"] = coldest_temperature_singles
        results["coldest_day_singles"] = coldest_day_singles
        results["warmest_day_singles"] = warmest_day_singles
        results["day_with_most_used_bikes_singles"] = day_with_most_used_bikes_singles
        results["correlation_matrix"] = correlation_matrix
        results["weekend_vs_weekday"] = weekend_vs_weekday
        results["weather_over_timestamp"] = weather_over_timestamp

        s3_handler.save_data_as_json(
            data=results,
            bucket_name=DESTINATION_BUCKET,
            path_name="graphs_data",
            sub_path="single_bikes",
            current_date=f"{current_date['min_date']}-{current_date['max_date']}",
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        raise e


def lambda_handler(event, context):
    process_station_data()
    process_single_data()
