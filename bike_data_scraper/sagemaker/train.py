# This is train.py
from sagemaker.sklearn.estimator import SKLearn
import os

role = "arn:aws:iam::796717305864:role/bike-scrapper-sagemaker-role"
training_job = os.path.join(
    os.getcwd(), "bike_data_scraper", "sagemaker", "training_job.py"
)

sklearn_estimator = SKLearn(
    entry_point=training_job,  # This should be your training and preprocessing script.
    role=role,
    instance_type="ml.m5.xlarge",
    framework_version="0.23-1",
    py_version="py3",
    hyperparameters={
        "bucket-name": "danneftw-dscrap-bucket",
        "file-path": "processed/station_bikes/2023-08-09-2023-11-02/StationaryStations.csv",
    },
)

sklearn_estimator.fit(
    {
        "train": "s3://danneftw-dscrap-bucket/processed/station_bikes/2023-08-09-2023-11-02/StationaryStations.csv"
    }
)
