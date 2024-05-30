from aws_cdk import Stage
from constructs import Construct

from infrastructure.data_scraper_stack import DataScraperStack


class DataScraperStage(Stage):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage_name: str,
        service_config: dict,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        stack_name = f"{stage_name}-DataScraperStack"
        DataScraperStack(
            self,
            "DataScraperStack",
            stage_name=stage_name,
            stack_name=stack_name,
            service_config=service_config,
        )
