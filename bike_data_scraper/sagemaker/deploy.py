import sagemaker
from sagemaker.sklearn.estimator import SKLearn
import boto3
import logging
import argparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True)

    args = parser.parse_args()
    role = "arn:aws:iam::796717305864:role/bike-scrapper-sagemaker-role"

    logging.info("Starting training from deploy.py")
    sagemaker_session = sagemaker.Session(
        boto_session=boto3.Session(region_name="eu-north-1")
    )

    logging.info("Reading train path data from s3")
    train_path = "/home/ec2-user/train.py"

    s3_input_train = sagemaker.inputs.TrainingInput(
        s3_data=f"s3://cyrille-dscrap-bucket/processed/{args.date}/station_bikes/",
        content_type="csv",
    )

    logging.info("Training model within deploy.py")
    sklearn = SKLearn(
        entry_point=train_path,
        instance_type="ml.m5.xlarge",
        role=role,
        sagemaker_session=sagemaker_session,
        framework_version="0.23-1",
        py_version="py3",
    )

    logging.info("Fitting model within deploy.py")
    sklearn.fit({"training": s3_input_train})

    logging.info("Deploying model within deploy.py")
    predictor = sklearn.deploy(
        instance_type="ml.m5.xlarge",
        initial_instance_count=1,
        endpoint_name="random-forest-endpoint-v2",
    )

    print(f"Endpoint successfully created \nName: {predictor.endpoint_name}")
