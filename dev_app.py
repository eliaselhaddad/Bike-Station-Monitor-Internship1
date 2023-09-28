#!/usr/bin/env python3
from os import getenv

import aws_cdk as cdk
from yaml import safe_load

from infrastructure.data_scraper_stack import DataScraperStack

stage_name = getenv("STAGE_NAME")
if not stage_name:
    raise Exception(
        "You need to set environment var 'STAGE_NAME' to be able to use and deploy the dev-app"
    )

with open("infrastructure/config/service-config.yaml") as file:
    service_config = safe_load(file)

env_dev = cdk.Environment(
    account=service_config["stages"]["dev"]["account"],
    region=service_config["stages"]["dev"]["region"],
)

app = cdk.App()
DataScraperStack(
    app,
    "DataScraperStack",
    stage_name=stage_name,
    env=env_dev,
    stack_name=f"{stage_name}-DataScraperStack",
    service_config=service_config,
)

app.synth()
