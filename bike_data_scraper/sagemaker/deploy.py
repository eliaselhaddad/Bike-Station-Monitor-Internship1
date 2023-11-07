import sagemaker
from sagemaker.sklearn.model import SKLearnModel
import os

if __name__ == "__main__":
    sagemaker_session = sagemaker.Session()
    role = "arn:aws:iam::796717305864:role/bike-scrapper-sagemaker-role"

    model_data = "s3://sagemaker-eu-north-1-796717305864/sagemaker-scikit-learn-2023-11-07-15-42-38-429/output/model.tar.gz"
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

    # Deploy the model to an endpoint with increased volume size
    predictor = sklearn_model.deploy(
        instance_type="ml.m5.2xlarge",
        initial_instance_count=1,
        endpoint_name="random-forest-endpoint-1",
    )

    print("Endpoint successfully created \nName: {}".format(predictor.endpoint_name))
