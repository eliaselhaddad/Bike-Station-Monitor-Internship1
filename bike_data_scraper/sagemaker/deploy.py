import sagemaker
from sagemaker import get_execution_role
from sagemaker.sklearn.estimator import SKLearn
import os
import boto3

if __name__ == "__main__":
    role = "arn:aws:iam::796717305864:role/bike-scrapper-sagemaker-role"
    sagemaker_session = sagemaker.Session(
        boto_session=boto3.Session(region_name="eu-north-1")
    )
    train_path = os.path.join(os.getcwd(), "bike_data_scraper", "sagemaker", "train.py")

    bucket_name = "sagemaker-eu-north-1-796717305864"
    xtrain_key = "sagemaker/sklearncontainer/xtrain.csv"
    xtest_key = "sagemaker/sklearncontainer/xtest.csv"
    ytrain_key = "sagemaker/sklearncontainer/ytrain.csv"
    ytest_key = "sagemaker/sklearncontainer/ytest.csv"

    sklearn = SKLearn(
        entry_point=train_path,
        role=role,
        instance_type="ml.m5.xlarge",
        sagemaker_session=sagemaker_session,
        framework_version="0.23-1",
        py_version="py3",
        hyperparameters={
            "bucket-name": bucket_name,
            "xtrain-key": xtrain_key,
            "xtest-key": xtest_key,
            "ytrain-key": ytrain_key,
            "ytest-key": ytest_key,
        },
    )

    sklearn.fit()

    predictor = sklearn.deploy(
        instance_type="ml.m5.xlarge",
        initial_instance_count=1,
        endpoint_name="random-forest-endpoint-1",
    )
