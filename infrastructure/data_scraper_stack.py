from aws_cdk import Stack
from constructs import Construct

from infrastructure.constructs.data_scraper import DataScraper


class DataScraperStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage_name: str,
        service_config: dict,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        DataScraper(
            self,
            f"{stage_name}-DataScraper",
            stage_name=stage_name,
            service_config=service_config,
        )
