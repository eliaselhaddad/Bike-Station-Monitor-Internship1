import http.client
import urllib.parse
import sagemaker
from sagemaker import get_execution_role
from sagemaker.sklearn.model import SKLearnPredictor
from predictor_handler import PredictorHandler
import json

API_ENDPOINT = "api.open-meteo.com"
PATH = "/v1/forecast"


def fetch_weather_data(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
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
            "Wind_Speed": hourly_data["windspeed_10m"][0],
            "Precipitation": hourly_data["precipitation"][0],
            "Visibility": hourly_data["visibility"][0],
            "Snowfall": hourly_data["snowfall"][0],
        }
    else:
        raise Exception("Failed to retrieve weather data.")


if __name__ == "__main__":
    LAT = "57.673431"
    LONG = "11.97932"

    weather_data = fetch_weather_data(LAT, LONG)

    input_df = PredictorHandler(
        IsOpen=True,
        Long=LONG,
        Lat=LAT,
        Year=2023,
        Month=11,
        Day=3,
        Hour=9,
        Minute=0,
        Temperature=weather_data["Temperature"],
        Humidity=weather_data["Humidity"],
        Wind_Speed=weather_data["Wind_Speed"],
        Precipitation=weather_data["Precipitation"],
        Visibility=weather_data["Visibility"],
        Snowfall=weather_data["Snowfall"],
        IsWeekend=0,
    ).create_dataframe()

    endpoint_name = "random-forest-endpoint-1"
    predictor = SKLearnPredictor(
        endpoint_name=endpoint_name, sagemaker_session=sagemaker.Session()
    )
    result = predictor.predict(input_df)
    print(f"Number of bikes at location is {round(result[0])}")
