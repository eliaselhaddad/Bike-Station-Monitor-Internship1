import boto3
import pytest


@pytest.fixture(scope="session")
def stage():
    return "bjad"


@pytest.fixture(scope="session")
def stack_exports(stage):
    test_session = boto3.Session(profile_name="lia-001")
    client = test_session.client("cloudformation")
    response = client.describe_stacks(StackName=f"{stage}-DataScraperStack")
    stack_outputs = response["Stacks"][0]["Outputs"]
    exports = {}
    for output in stack_outputs:
        if "ExportName" in output:
            key = output["ExportName"]
            value = output["OutputValue"]
            exports[key] = value

    return exports
