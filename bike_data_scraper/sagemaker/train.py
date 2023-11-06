from sagemaker.sklearn.estimator import SKLearn
import sagemaker
import os

# Set your script parameters (if you have any)
script_params = {
    "bucket-name": "sagemaker-eu-north-1-796717305864",
    "xtrain-key": "sagemaker/sklearncontainer/xtrain2.csv",
    "xtest-key": "sagemaker/sklearncontainer/xtest2.csv",
    "ytrain-key": "sagemaker/sklearncontainer/ytrain2.csv",
    "ytest-key": "sagemaker/sklearncontainer/ytest2.csv",
}

# Create an SKLearn estimator
estimator = SKLearn(
    entry_point=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "training_job.py"
    ),  # Your script filename
    framework_version="0.23-1",  # Version of scikit-learn you want to use
    role="AmazonSageMaker-ExecutionRole-20231005T090396",  # IAM role ARN
    instance_count=1,  # Number of instances to use
    instance_type="ml.m5.4xlarge",  # Specify a larger instance type here
    sagemaker_session=sagemaker.Session(),  # Session object
    hyperparameters=script_params,  # Pass script parameters (if any)
)

# Fit the model
estimator.fit({"train": "s3://sagemaker-eu-north-1-796717305864"})
