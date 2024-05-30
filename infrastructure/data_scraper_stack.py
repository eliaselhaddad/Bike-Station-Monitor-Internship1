from aws_cdk import Stack, aws_lambda, RemovalPolicy
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from constructs import Construct

from infrastructure.constructs.cognito_user_pool import CognitoUserPool
from infrastructure.constructs.data_scraper import DataScraper
from infrastructure.constructs.data_fetch_and_save import SaveTwoWeeksBikeDataToCsv
from infrastructure.constructs.two_weeks_processed_data import DataPreprocessed
from infrastructure.constructs.step_functions_poc import StepFunctionsPoc
from infrastructure.constructs.weather_data_scraper import WeatherDataScraper
from infrastructure.constructs.graphs_data_scraper import GraphsDataScraper

# from infrastructure.constructs.training_data_split import TrainingDataSplitScraper
from infrastructure.constructs.start_ec2_instance import StartEC2TrainingInstance
from infrastructure.constructs.check_ec2_status import CheckEC2TrainingInstance
from infrastructure.constructs.shutdown_ec2_training_instance import (
    ShutdownEC2TrainingInstance,
)


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

        # training_layer = PythonLayerVersion(
        #     self,
        #     training_layer_name,
        #     layer_version_name=training_layer_name,
        #     entry=".build/ml_layer/",
        #     compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_10],
        #     removal_policy=RemovalPolicy.DESTROY,
        # )

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
            lambda_layer=common_layer,
        )

        CognitoUserPool(
            self,
            f"{stage_name}-CognitoUserPool",
            stage_name=stage_name,
            service_config=service_config,
            lambda_common_layer=common_layer,
        )
        DataPreprocessed(
            self,
            f"{stage_name}-DataPreprocessed",
            stage_name=stage_name,
            service_config=service_config,
            lambda_layer=common_layer,
        )
        StepFunctionsPoc(
            self,
            f"{stage_name}-StepFunctionsPoc",
            stage_name=stage_name,
            service_config=service_config,
            lambda_layer=common_layer,
        )
        GraphsDataScraper(
            self,
            f"{stage_name}-GraphsDataScraper",
            stage_name=stage_name,
            service_config=service_config,
            lambda_layer=common_layer,
        )

        SaveTwoWeeksBikeDataToCsv(
            self,
            f"{stage_name}-SaveTwoWeeksBikeDataToCsv",
            stage_name=stage_name,
            service_config=service_config,
            lambda_layer=common_layer,
        )

        StartEC2TrainingInstance(
            self,
            f"{stage_name}-StartEC2TrainingInstance",
            stage_name=stage_name,
            service_config=service_config,
            lambda_layer=common_layer,
        )

        CheckEC2TrainingInstance(
            self,
            f"{stage_name}-CheckEC2TrainingInstance",
            stage_name=stage_name,
            service_config=service_config,
            lambda_layer=common_layer,
        )

        ShutdownEC2TrainingInstance(
            self,
            f"{stage_name}-ShutdownEC2TrainingInstance",
            stage_name=stage_name,
            service_config=service_config,
            lambda_layer=common_layer,
        )
