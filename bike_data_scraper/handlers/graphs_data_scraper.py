import os
import boto3
from datetime import datetime
from loguru import logger
import pandas as pd
from bike_data_scraper.s3_client.s3_handler import S3Handler
from bike_data_scraper.libs.functions import get_min_and_max_dates_from_dataframe

SOURCE_BUCKET = os.environ.get("GRAPHS_SOURCE_BUCKET")
DESTINATION_BUCKET = os.environ.get("GRAPHS_DESTINATION_BUCKET")

SINGLE_BIKE_DATA_KEY = os.environ.get("SINGLE_BIKE_DATA")
STATION_BIKE_DATA_KEY = os.environ.get("STATION_BIKE_DATA")

AWS_TMP_DIR = "/tmp/"
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


def number_of_bikes_available(df: pd.DataFrame) -> str:
    try:
        logger.info("Calculating number of bikes available")
        number_of_unique_bikes_in_dataset = df["stationId"].nunique()
        unique_bike_object = f"{number_of_unique_bikes_in_dataset}"
        return unique_bike_object
    except Exception as e:
        logger.error(f"Error calculating number of bikes available: {e}")
        raise e


def warmest_temperature(df: pd.DataFrame) -> str:
    try:
        logger.info("Calculating warmest temperature")
        warmest_temperature = df["Temperature"].max()
        warmest_temperature_object = f"{warmest_temperature}"
        return warmest_temperature_object
    except Exception as e:
        logger.error(f"Error calculating warmest temperature: {e}")
        raise e


def coldest_temperature(df: pd.DataFrame) -> str:
    try:
        logger.info("Calculating coldest temperature")
        coldest_temperature = df["Temperature"].min()
        coldest_temperature_object = f"{coldest_temperature}"
        return coldest_temperature_object
    except Exception as e:
        logger.error(f"Error calculating coldest temperature: {e}")
        raise e


def coldest_day(df: pd.DataFrame) -> str:
    try:
        logger.info("Calculating coldest day")
        coldest_day = df["timestamp"].loc[df["Temperature"].idxmin()]
        coldest_day_object = f"{coldest_day}"
        return coldest_day_object
    except Exception as e:
        logger.error(f"Error calculating coldest day: {e}")
        raise e


def warmest_day(df: pd.DataFrame) -> str:
    try:
        logger.info("Calculating warmest day")
        warmest_day = df["timestamp"].loc[df["Temperature"].idxmax()]
        warmest_day_object = f"{warmest_day}"
        return warmest_day_object
    except Exception as e:
        logger.error(f"Error calculating warmest day: {e}")
        raise e


def day_with_most_used_bikes(df: pd.DataFrame) -> str:
    try:
        logger.info("Calculating day with most used bikes")
        day_with_most_used_bikes = df["timestamp"].value_counts().idxmax()
        day_with_most_used_bikes_object = f"{day_with_most_used_bikes}"
        return day_with_most_used_bikes_object
    except Exception as e:
        logger.error(f"Error calculating day with most used bikes: {e}")
        raise e


def correlation_matrix_plot(df: pd.DataFrame) -> pd.DataFrame:
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
    correlation_matrix.to_csv(f"{AWS_TMP_DIR}CorrelationMatrix.csv")
    return correlation_matrix


def weekend_vs_weekday_plot(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Calculating weekend vs weekday plot")
    average_number_of_bikes = (
        df.groupby("IsWeekend")["AvailableBikes"].mean().reset_index()
    )
    average_number_of_bikes["IsWeekend"] = average_number_of_bikes["IsWeekend"].replace(
        {0: "Weekdays", 1: "Weekend"}
    )
    average_number_of_bikes.to_csv(
        f"{AWS_TMP_DIR}WeekendVsWeekdaysSingles.csv", index=False
    )
    return average_number_of_bikes


def weather_over_timestamp_plot(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Calculating weather over timestamp plot")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
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
    grouped_df.to_csv(f"{AWS_TMP_DIR}WeatherOverTimeStations.csv", index=False)
    return grouped_df


def calculate_metrics(bike_data):
    metrics = {}
    logger.info("Start calculating metrics for dataset")
    metrics["number_of_bikes_available"] = number_of_bikes_available(bike_data)
    metrics["warmest_temperature"] = warmest_temperature(bike_data)
    metrics["coldest_temperature"] = coldest_temperature(bike_data)
    metrics["coldest_day"] = coldest_day(bike_data)
    metrics["warmest_day"] = warmest_day(bike_data)
    metrics["day_with_most_used_bikes"] = day_with_most_used_bikes(bike_data)
    metrics["correlation_matrix"] = correlation_matrix_plot(bike_data)
    metrics["weekend_vs_weekday"] = weekend_vs_weekday_plot(bike_data)
    metrics["weather_over_timestamp"] = weather_over_timestamp_plot(bike_data)
    logger.info("Finished calculating metrics for dataset")
    return metrics


def package_results(metrics):
    results = {}
    for key, value in metrics.items():
        logger.info(f"Packaging results for {key}")
        results[key] = value
    logger.info("Finished packaging results for dataset")
    return results


def save_results(results, data_sub_folder: str, sub_path: str):
    s3_handler.save_data_as_json(
        data=results,
        bucket_name=DESTINATION_BUCKET,
        path_name="graphs_data",
        sub_path=sub_path,
        current_date=data_sub_folder,
    )


def lambda_handler(event, context):
    try:
        station_bikes_data = get_weather_data(SOURCE_BUCKET, STATION_BIKE_DATA_KEY)
        single_bikes_data = get_weather_data(SOURCE_BUCKET, SINGLE_BIKE_DATA_KEY)

        current_date = get_min_and_max_dates_from_dataframe(station_bikes_data)
        date = f"{current_date['min_date']}-{current_date['max_date']}"

        metrics_station_bikes = calculate_metrics(station_bikes_data)
        metrics_single_bikes = calculate_metrics(single_bikes_data)

        results_station_bikes = package_results(metrics_station_bikes)
        results_single_bikes = package_results(metrics_single_bikes)

        save_results(
            results=results_station_bikes,
            data_sub_folder=date,
            sub_path="station_bikes",
        )

        save_results(
            results=results_single_bikes,
            data_sub_folder=date,
            sub_path="single_bikes",
        )
    except Exception as e:
        logger.error(f"Error saving graph data into S3: {e}")
