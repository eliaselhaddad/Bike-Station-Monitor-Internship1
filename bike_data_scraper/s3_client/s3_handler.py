from datetime import datetime
from io import StringIO
import boto3
import pandas as pd


class S3Handler:
    def __init__(self, bucket_name) -> None:
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.current_date = datetime.now().strftime("%d-%m-%Y")

    def save_dataframe_to_s3(
        self,
        df: pd.DataFrame,
        bucket_name: str,
        path_name: str,
        current_date: str,
        filename: str,
        sub_path: str,
    ):
        with StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            self.s3_resource.Object(
                bucket_name, f"{path_name}/{sub_path}/{current_date}/{filename}.csv"
            ).put(Body=csv_buffer.getvalue())

    def get_data_from_s3(self, bucket_name: str, key: str) -> pd.DataFrame:
        obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(obj["Body"])
        return df

    def put_in_s3_bucket(self, filename, data_to_save):
        self.s3_client.put_object(
            Bucket=self.bucket_name, Key=filename, Body=data_to_save
        )
        return filename