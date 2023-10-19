from datetime import datetime
from io import StringIO
import boto3
import pandas as pd
from loguru import logger
import json


class S3Handler:
    def __init__(self) -> None:
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
        if df.empty:
            logger.error(f"No data found for bucket {bucket_name} and key {key}")
            raise Exception("No data found in S3 bucket")
        return df

    def put_in_s3_bucket(self, filename: str, data_to_save, bucket_name: str):
        self.s3_client.put_object(Bucket=bucket_name, Key=filename, Body=data_to_save)
        return filename

    def convert_dataframe_to_dict(self, data: dict) -> dict:
        try:
            for key, value in data.items():
                if isinstance(value, pd.DataFrame):
                    for col in value.columns:
                        try:
                            if pd.api.types.is_datetime64_any_dtype(value[col]):
                                value[col] = value[col].astype(str)
                        except Exception as e:
                            logger.error(
                                f"Error in checking datetime dtype for column {col}: {e}"
                            )
                    data[key] = value.to_dict(orient="records")
            return data
        except Exception as e:
            logger.error(f"Error in convert_dataframe_to_dict: {e}")
            raise e

    def save_data_as_json(
        self,
        data: dict,
        bucket_name: str,
        path_name: str,
        sub_path: str,
        current_date: str,
    ) -> None:
        try:
            logger.info(f"Converting {sub_path} dataframe data to dict")
            data = self.convert_dataframe_to_dict(data)

            logger.info(f"Saving {sub_path} graph json data to S3")
            self.s3_client.put_object(
                Body=json.dumps(data),
                Bucket=bucket_name,
                Key=f"{path_name}/{sub_path}/{current_date}/data.json",
            )
            logger.info("Successfully saved data to S3")
        except Exception as e:
            logger.error(f"Error for save_data_as_json: {e}")
            raise e

    # def save_data_as_json(
    #     self,
    #     data: dict,
    #     bucket_name: str,
    #     path_name: str,
    #     sub_path: str,
    #     current_date: str,
    # ) -> None:
    #     logger.info(f"Saving {sub_path} graph json data to S3")

    #     for key, value in data.items():
    #         if isinstance(value, pd.DataFrame):
    #             for col in value.columns:
    #                 if pd.api.types.is_datetime64_any_dtype(value[col]) == True:
    #                     value[col] = value[col].astype(str)
    #             data[key] = value.to_dict(orient="records")

    #     self.s3_client.put_object(
    #         Body=json.dumps(data),
    #         Bucket=bucket_name,
    #         Key=f"{path_name}/{sub_path}/{current_date}/data.json",
    #     )
