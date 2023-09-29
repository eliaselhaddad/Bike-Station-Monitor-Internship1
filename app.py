import aws_cdk as cdk
from yaml import safe_load

from infrastructure.ci.data_scraper_pipeline_stack import DeviceManagerPipelineStack

with open("infrastructure/config/service-config.yaml") as file:
    service_config = safe_load(file)

env_build = cdk.Environment(
    account=service_config["pipeline"]["account"],
    region=service_config["pipeline"]["region"],
)

app = cdk.App()
DeviceManagerPipelineStack(
    app,
    "DataManagerPipelineStack",
    repository_arn=service_config["pipeline"]["repository_arn"],
    env=env_build,
    stack_name="DataScraperPipelineStack",
    service_config=service_config,
)

app.synth()
