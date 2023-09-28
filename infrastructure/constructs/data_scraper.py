import aws_cdk
from aws_cdk import (
    aws_dynamodb,
    aws_lambda
)

from constructs import Construct


class DataScraper(Construct):
    def __init__(
        self, scope: Construct, id_: str, stage_name: str, service_config: dict
    ) -> None:
        super().__init__(scope, id_)

        service_name = service_config["service"]["service_name"]
        service_short_name = service_config["service"]["service_short_name"]

        #Define the data scraper, so lambda functions dynamoDB tables etc