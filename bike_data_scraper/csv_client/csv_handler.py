import csv
from io import StringIO
import pandas as pd


class UniversalCSVConverter:
    def __init__(self, columns=None, data=None):
        self.columns = columns
        self.data = data

    def set_data(self, data):
        self.data = data  # sets

    def set_columns(self, columns):
        self.columns = columns

    @staticmethod
    def convert_to_csv(columns, data):
        csv_file = StringIO()
        csv_writer = csv.DictWriter(csv_file, fieldnames=columns)
        csv_writer.writeheader()
        for item in data:
            csv_writer.writerow(item)
        csv_data = csv_file.getvalue()
        csv_file.close()
        return csv_data

    def to_csv(self):  # using instance variables
        if self.data is None:
            raise ValueError("No data provided")

        if isinstance(self.data, pd.DataFrame):
            return self.data.to_csv(index=False, columns=self.columns)

        elif isinstance(self.data, list) and all(
            isinstance(item, dict) for item in self.data
        ):
            columns = self.columns or list(self.data[0].keys())
            print(self.data[0].keys())
            print(self.data)
            return self.convert_to_csv(columns, self.data)

        else:
            raise ValueError("Unsupported data type")
