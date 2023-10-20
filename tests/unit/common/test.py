from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from freezegun import freeze_time


# "erik-admin"
def test():
    input = "erik"
    result = concat_admin_to_string(input)
    assert len(result) == 10
    assert result.split("-", 5)[1] == "admin"


def concat_admin_to_string(input: str) -> str:
    return f"{input}-admin"
