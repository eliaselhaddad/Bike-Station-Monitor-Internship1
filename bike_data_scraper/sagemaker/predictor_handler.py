import pandas as pd
import requests
from datetime import datetime


class PredictorHandler:
    def __init__(
        self,
        IsOpen,
        Long,
        Lat,
        Year,
        Month,
        Day,
        Hour,
        Minute,
        Temperature,
        Humidity,
        Wind_Speed,
        Precipitation,
        Visibility,
        Snowfall,
        IsWeekend,
    ):
        self.IsOpen = IsOpen
        self.Long = Long
        self.Lat = Lat
        self.Year = Year
        self.Month = Month
        self.Day = Day
        self.Hour = Hour
        self.Minute = Minute

        self.Temperature = Temperature
        self.Humidity = Humidity
        self.Wind_Speed = Wind_Speed
        self.Precipitation = Precipitation
        self.Visibility = Visibility
        self.Snowfall = Snowfall
        self.IsWeekend = IsWeekend

    def create_dataframe(self):
        df = pd.DataFrame(
            {
                "IsOpen": [self.IsOpen],
                "Long": [self.Long],
                "Lat": [self.Lat],
                "Year": [self.Year],
                "Month": [self.Month],
                "Day": [self.Day],
                "Hour": [self.Hour],
                "Minute": [self.Minute],
                "Temperature": [self.Temperature],
                "Humidity": [self.Humidity],
                "Wind_Speed": [self.Wind_Speed],
                "Precipitation": [self.Precipitation],
                "Visibility": [self.Visibility],
                "Snowfall": [self.Snowfall],
                "IsWeekend": [self.IsWeekend],
            }
        )

        return df
