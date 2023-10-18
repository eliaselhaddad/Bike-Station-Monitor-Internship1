import csv
from io import StringIO
import pandas as pd


class UniversalCSVConverter:
    def __init__(self, columns=None, data=None):
        self.columns = columns
        self.data = data

    def set_data(self, data):
        self.data = data

    def set_columns(self, columns):
        self.columns = columns

    def to_csv(self):
        if not self.data:
            raise ValueError("No data provided")

        if isinstance(self.data, list) and all(
            isinstance(item, dict) for item in self.data
        ):
            return self._dict_list_to_csv(self.data)
        elif isinstance(self.data, pd.DataFrame):
            return self.data.to_csv(index=False)
        else:
            raise ValueError("Unsupported data type")

    def _dict_list_to_csv(self, items):
        csv_file = StringIO()
        csv_writer = csv.DictWriter(csv_file, fieldnames=self.columns)
        csv_writer.writeheader()
        for item in items:
            csv_writer.writerow(item)

        csv_data = csv_file.getvalue()
        csv_file.close()
        return csv_data
