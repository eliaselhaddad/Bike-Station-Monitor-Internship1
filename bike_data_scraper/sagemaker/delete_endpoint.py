# this script is to make sure that there is no endpint or endpoint config activate
# if the endpoint and config is active it'll cost us money unecessarily
# run script when you're done testing and using sagemaker model

import boto3


def delete_sagemaker_endpoint(endpoint_name):
    """
    Delete a SageMaker endpoint and its associated configuration.
    Args:
    - endpoint_name (str): Name of the SageMaker endpoint to be deleted.
    """
    # Initialize the SageMaker client
    sagemaker_client = boto3.client("sagemaker")

    # Delete the endpoint
    try:
        sagemaker_client.delete_endpoint(EndpointName=endpoint_name)
        print(f"Deleted endpoint: {endpoint_name}")
    except Exception as e:
        print(f"Error deleting endpoint {endpoint_name}: {e}")

    # Delete the endpoint configuration
    try:
        sagemaker_client.delete_endpoint_config(EndpointConfigName=endpoint_name)
        print(f"Deleted endpoint configuration: {endpoint_name}")
    except Exception as e:
        print(f"Error deleting endpoint configuration {endpoint_name}: {e}")


# Usage
endpoint_name_to_delete = (
    "random-forest-endpoint-1"  # Replace with your SageMaker endpoint name
)
delete_sagemaker_endpoint(endpoint_name_to_delete)
