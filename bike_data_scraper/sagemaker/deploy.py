import sagemaker
from sagemaker.sklearn.model import SKLearnModel
import boto3
import os

if __name__ == "__main__":
    boto_session = boto3.Session(region_name="eu-north-1")
    sagemaker_session = sagemaker.Session(boto_session=boto_session)

    role = "arn:aws:iam::796717305864:role/bike-scrapper-sagemaker-role"

    model_data = "s3://sagemaker-eu-north-1-796717305864/sagemaker-scikit-learn-2023-11-09-09-16-37-966/output/model.tar.gz"
    inference_script_path = os.path.join(
        os.getcwd(), "bike_data_scraper", "sagemaker", "inference.py"
    )

    sklearn_model = SKLearnModel(
        model_data=model_data,
        role=role,
        framework_version="0.23-1",
        py_version="py3",
        entry_point=inference_script_path,
    )

    # Deploy model with the specified instance type
    # Real-time inference configuration
    predictor = sklearn_model.deploy(
        instance_type="ml.r5.large",
        initial_instance_count=1,
        endpoint_name="random-forest-endpoint-1",
    )

    print(f"Endpoint successfully created \nName: {predictor.endpoint_name}")
