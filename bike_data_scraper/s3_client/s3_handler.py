from datetime import datetime
from io import StringIO

import boto3
import pandas as pd
from loguru import logger


class S3Handler:
    def __init__(self) -> None:
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.current_date = datetime.now().strftime("%d-%m-%Y")

    def save_dataframe_to_s3(
        self, df: pd.DataFrame, bucket_name: str, path_name: str, filename: str
    ):
        with StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            self.s3_resource.Object(
                bucket_name, f"{path_name}/{self.current_date}/{filename}.csv"
            ).put(Body=csv_buffer.getvalue())

    def get_data_from_s3(self, bucket_name: str, key: str) -> pd.DataFrame:
        obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(obj["Body"])
        return df
