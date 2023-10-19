import csv
from io import StringIO
import pandas as pd


class UniversalCSVConverter:
    def __init__(self, columns=None, data=None):
        self.columns = columns
        self.data = data

    def set_data(self, data):
        self.data = data  # lets us change the data after instantiation

    def set_columns(self, columns):
        self.columns = columns  # same as above but for columns

    @staticmethod  # doesn't require self as an argument
    # can use convert_to_csv without creating an instance of the class
    # can't modify object state nor class state
    def convert_to_csv(columns, data):
        csv_file = StringIO()
        csv_writer = csv.DictWriter(csv_file, fieldnames=columns)
        csv_writer.writeheader()
        for item in data:
            csv_writer.writerow(item)
        csv_data = csv_file.getvalue()
        csv_file.close()
        return csv_data

    def to_csv(self):
        if self.data is None:
            raise ValueError("No data provided")

        if isinstance(self.data, pd.DataFrame):  # for pandas dataframes
            return self.data.to_csv(
                index=False, columns=self.columns
            )  # "to_csv" is a pandas method here

        elif isinstance(self.data, list) and all(  # for lists of dictionaries
            isinstance(item, dict) for item in self.data
        ):
            columns = self.columns or list(
                self.data[0].keys()
            )  # keys in this case are the column names
            return self.convert_to_csv(columns, self.data)

        else:
            raise ValueError("Unsupported data type")
