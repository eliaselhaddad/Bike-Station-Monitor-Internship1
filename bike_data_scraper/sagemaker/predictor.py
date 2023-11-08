import http.client
import urllib.parse
import sagemaker
from sagemaker.sklearn.model import SKLearnPredictor
from predictor_handler import PredictorHandler
import json
from datetime import datetime

API_ENDPOINT = "api.open-meteo.com"
PATH = "/v1/forecast"


def fetch_weather_data():
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m,precipitation,visibility,snowfall",
    }
    params_str = urllib.parse.urlencode(params)
    url = f"{PATH}?{params_str}"

    conn = http.client.HTTPSConnection(API_ENDPOINT)
    conn.request("GET", url)
    response = conn.getresponse()

    if response.status == 200:
        response_data = response.read()
        weather_data = json.loads(response_data)
        hourly_data = weather_data["hourly"]
        return {
            "Temperature": hourly_data["temperature_2m"][0],
            "Humidity": hourly_data["relativehumidity_2m"][0],
            "Windspeed": hourly_data["windspeed_10m"][0],
            "Precipitation": hourly_data["precipitation"][0],
            "Visibility": hourly_data["visibility"][0],
            "Snowfall": hourly_data["snowfall"][0],
        }
    else:
        raise Exception("Failed to retrieve weather data.")


if __name__ == "__main__":
    LAT = 57.69669
    LONG = 11.972278
    weather_data = fetch_weather_data()

    # Get current system time
    now = datetime.now()

    input_df = PredictorHandler(
        IsOpen=True,
        Long=LONG,
        Lat=LAT,
        Year=now.year,
        Month=now.month,
        Day=now.day,
        Hour=now.hour,
        Minute=now.minute,
        Temperature=weather_data["Temperature"],
        Humidity=weather_data["Humidity"],
        Windspeed=weather_data["Windspeed"],
        Precipitation=weather_data["Precipitation"],
        Visibility=weather_data["Visibility"],
        Snowfall=weather_data["Snowfall"],
        IsWeekend=int(now.weekday() >= 5),  # 0 for weekdays, 1 for weekends
    ).create_dataframe()

    endpoint_name = "random-forest-endpoint-1"
    predictor = SKLearnPredictor(
        endpoint_name=endpoint_name, sagemaker_session=sagemaker.Session()
    )
    result = predictor.predict(input_df)
    print(f"Number of bikes at location is {round(result[0])}")
