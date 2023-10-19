from aws_cdk import Stack, aws_lambda, RemovalPolicy
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from constructs import Construct

from infrastructure.constructs.cognito_user_pool import CognitoUserPool
from infrastructure.constructs.data_scraper import DataScraper
from infrastructure.constructs.weather_data_scraper import WeatherDataScraper


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

        service_short_name = service_config["service"]["service_short_name"]
        common_layer_name = f"{stage_name}-{service_short_name}-common-layer"

        common_layer = PythonLayerVersion(
            self,
            common_layer_name,
            layer_version_name=common_layer_name,
            entry=".build/common_layer/",
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_10],
            removal_policy=RemovalPolicy.DESTROY,
        )

        DataScraper(
            self,
            f"{stage_name}-DataScraper",
            stage_name=stage_name,
            service_config=service_config,
            lambda_layer=common_layer,
        )

        WeatherDataScraper(
            self,
            f"{stage_name}-WeatherDataScraper",
            stage_name=stage_name,
            service_config=service_config,
        )

        CognitoUserPool(
            self,
            f"{stage_name}-CognitoUserPool",
            stage_name=stage_name,
            service_config=service_config,
            lambda_common_layer=common_layer,
        )
