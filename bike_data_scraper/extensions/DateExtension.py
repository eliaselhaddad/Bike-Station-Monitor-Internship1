from datetime import datetime, timedelta, date
from typing import Dict
from zoneinfo import ZoneInfo


class DateExtension:
    def __init__(self):
        self.stockholm_time_zone = ZoneInfo("Europe/Stockholm")

    def get_datetime_now_cet(self) -> datetime:
        return datetime.now(self.stockholm_time_zone).replace(tzinfo=None)

    @staticmethod
    def get_date_yesterday_cet_to_string(self) -> str:
        yesterday = datetime.now(self.stockholm_time_zone) - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d")

    @staticmethod
    def get_14_days_date_from_yesterday_to_string(self) -> Dict:
        yesterday = datetime.now(self.stockholm_time_zone) - timedelta(days=1)
        fourteen_days_ago_date = yesterday - timedelta(days=13)
        return {
            "yesterday": yesterday.strftime("%Y-%m-%d"),
            "fourteen_days_ago": fourteen_days_ago_date.strftime("%Y-%m-%d"),
        }

    @staticmethod
    def get_14_days_date_from_datetime_to_string(starting_datetime: datetime) -> str:
        fourteen_days_ago_date = starting_datetime - timedelta(days=13)
        return f"{starting_datetime.strftime('%Y-%m-%d')}-{fourteen_days_ago_date.strftime('%Y-%m-%d')}"


@staticmethod
def get_14_days_date(starting_date: datetime) -> tuple[date, date]:
    return starting_date.date(), (starting_date - timedelta(days=13)).date()
