import csv
from io import StringIO
import pandas as pd
import pytest


class UniversalCSVConverter:
    # initialize the class with the columns and data
    def __init__(self, columns, data):
        self.columns = columns
        self.data = data

    def set_data(self, data):  # updates data attribute
        self.data = data

    def set_columns(self, columns):  # same as above but with columns
        self.columns = columns

    @staticmethod
    def convert_to_csv(
        columns, data
    ):  # takes columns and data (list of dicts) and returns csv
        csv_file = StringIO()
        csv_writer = csv.DictWriter(csv_file, fieldnames=columns)
        csv_writer.writeheader()
        for item in data:
            csv_writer.writerow(item)

        csv_data = csv_file.getvalue()
        csv_file.close()
        return csv_data

    def to_csv(self):
        if self.data is None:  # if data None raise error
            raise ValueError("No data provided")

        if isinstance(self.data, list) and all(  # if data is a list of dicts
            isinstance(item, dict) for item in self.data
        ):
            return self.convert_to_csv(self.columns, self.data)
        elif isinstance(self.data, pd.DataFrame):  # if data is a pandas dataframe
            return self.data.to_csv(index=False)
        else:
            raise ValueError("Unsupported data type")
