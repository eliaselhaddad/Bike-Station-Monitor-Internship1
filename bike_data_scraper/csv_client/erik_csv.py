from datetime import datetime
from io import StringIO
import csv
from typing import List, Dict
import re

import pydantic as pydantic


class BikeDataModel(pydantic.BaseModel):
    stationId: str
    timestamp: datetime
    availableBikes: str


class ErikCsv:
    def dataframe_to_csv(self):
        """ """
        print("o")

    def dynamodb_items_to_csv(self, dynamodb_items: List[Dict[str, str]]) -> str:
        csv_file = StringIO()
        csv_writer = csv.DictWriter(csv_file, fieldnames=dynamodb_items[0].keys())
        csv_writer.writeheader()
        for item in dynamodb_items:
            csv_writer.writerow(item)
        csv_data = csv_file.getvalue()
        csv_file.close()
        return csv_data


items = [{"stationId": "1"}]


# as byte
# stationId\r\n1\r\n


def test():
    result = ErikCsv().dynamodb_items_to_csv(items)
    assert bytes(b"stationId\r\n1\r\n") == result.encode()
