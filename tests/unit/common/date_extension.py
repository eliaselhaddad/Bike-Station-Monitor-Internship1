from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from freezegun import freeze_time


@freeze_time("2023-10-14 00:00:00")
def test_time_get_date_now_cest_to_string():
    result = DateExtension().get_date_now_cest_to_string()
    assert len(result) == 10
    assert result == "2023-10-14"


@freeze_time("2023-10-14 00:00:00")
def test_get_datetime_now_cest():
    now_stockholm_datetime = DateExtension().get_datetime_now_cest()

    assert datetime.now() + timedelta(hours=2) == now_stockholm_datetime
    assert (
        datetime.strptime("2023-10-14 02:00:00", "%Y-%m-%d %H:%M:%S")
        == now_stockholm_datetime
    )


@freeze_time("2023-10-14")
def test_get_14_days_date_from_datetime_to_string():
    date_time = datetime.now(ZoneInfo("Europe/Stockholm"))
    fourteen_days_date = DateExtension.get_14_days_date_from_datetime_to_string(
        date_time
    )
    assert fourteen_days_date == "2023-10-14-2023-10-01"


@freeze_time("2023-10-14")
def test_get_14_days_date_from_datetime():
    date_parameter = datetime.now(ZoneInfo("Europe/Stockholm"))
    starting_date, fourteen_days_ago = DateExtension.get_14_days_date(date_parameter)
    assert starting_date == date(2023, 10, 14)
    assert fourteen_days_ago == date(2023, 10, 1)


class DateExtension:
    def __init__(self):
        self.stockholm_time_zone = ZoneInfo("Europe/Stockholm")

    def get_datetime_now_cest(self) -> datetime:
        return datetime.now(self.stockholm_time_zone).replace(tzinfo=None)

    def get_date_now_cest_to_string(self) -> str:
        return datetime.now(self.stockholm_time_zone).strftime("%Y-%m-%d")

    @staticmethod
    def get_14_days_date_from_datetime_to_string(starting_datetime: datetime) -> str:
        fourteen_days_ago_date = starting_datetime - timedelta(days=13)
        return f"{starting_datetime.strftime('%Y-%m-%d')}-{fourteen_days_ago_date.strftime('%Y-%m-%d')}"

    @staticmethod
    def get_14_days_date(starting_date: datetime) -> tuple[date, date]:
        return starting_date.date(), (starting_date - timedelta(days=13)).date()
