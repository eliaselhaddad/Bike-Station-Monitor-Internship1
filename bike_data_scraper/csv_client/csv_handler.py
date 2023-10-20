import csv
from io import StringIO


class CSVHandler:
    def __init__(self):
        pass

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
