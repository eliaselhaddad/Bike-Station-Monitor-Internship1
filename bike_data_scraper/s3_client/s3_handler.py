import boto3


class S3PutInBucketHandler:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3")

    def put_in_s3_bucket(self, filename, data_to_save):
        self.s3.put_object(Bucket=self.bucket_name, Key=filename, Body=data_to_save)
        return filename
