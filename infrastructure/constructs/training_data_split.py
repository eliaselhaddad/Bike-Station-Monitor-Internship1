import builtins
from aws_cdk import (
    aws_lambda,
    aws_iam,
    Duration,
    aws_logs,
    aws_sagemaker,
)
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion

from constructs import Construct
import os

from typing import List


class TrainingDataSplitScraper(Construct):
    def __init__(
        self,
        scope: Construct,
        id_: str,
        stage_name: str,
        service_config: dict,
        lambda_layer: List[PythonLayerVersion],
    ) -> None:
        super().__init__(scope, id_)

        service_short_name = service_config["service"]["service_short_name"]
        service_name = service_short_name
        cwd = os.getcwd()

        self.lambda_layer = lambda_layer
        s3_name_prefix = f"{stage_name}-{service_short_name}"

        # lambda setup
        lambda_name = f"{stage_name}-{service_short_name}-training-data-split"
        lambda_role_name = f"{lambda_name}-role"
        self.lambda_role = self._build_lambda_role(role_name=lambda_role_name)

        env_vars = self._create_env_vars(
            stage_name=stage_name,
            service_name=service_name,
            s3_destination_bucket=f"training-data-split-bucket",
            s3_source_bucket=f"training-data-split-bucket",
        )

        self.training_data_split_lambda = self._build_lambda(
            lambda_name=lambda_name,
            stage_name=stage_name,
            env_vars=env_vars,
            cwd=cwd,
        )

    @staticmethod
    def _create_env_vars(
        stage_name: str,
        service_name: str,
        s3_source_bucket: str,
        s3_destination_bucket: str,
    ) -> dict:
        return {
            "STAGE_NAME": stage_name,
            "SERVICE_NAME": service_name,
            "TRAINING_DATA_SPLIT_SOURCE_BUCKET": s3_source_bucket,
            "TRAINING_DATA_SPLIT_DESTINATION_BUCKET": s3_destination_bucket,
            "STATION_BIKE_DATA": "processed/station_bikes/2023-09-10-2023-09-24/StationaryStations.csv",
            "SINGLE_BIKE_DATA": "processed/single_bikes/2023-09-10-2023-09-24/SingleBikes.csv",
            "LOG_LEVEL": "DEBUG",
        }

    def _build_lambda(
        self,
        lambda_name: str,
        stage_name: str,
        env_vars: dict,
        cwd: str,
    ) -> aws_lambda.Function:
        return aws_lambda.Function(
            self,
            id=lambda_name,
            function_name=lambda_name,
            runtime=aws_lambda.Runtime.PYTHON_3_10,
            code=aws_lambda.Code.from_asset(path=os.path.join(cwd, ".build/lambdas/")),
            handler="bike_data_scraper/handlers/train_data_split.lambda_handler",
            tracing=aws_lambda.Tracing.ACTIVE,
            retry_attempts=2,
            role=self.lambda_role,
            timeout=Duration.seconds(900),
            environment=env_vars,
            layers=self.lambda_layer,
            log_retention=aws_logs.RetentionDays.ONE_WEEK,
            memory_size=1024,
        )

    def _build_lambda_role(self, role_name: str) -> aws_iam.Role:
        return aws_iam.Role(
            self,
            id=role_name,
            role_name=role_name,
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3FullAccess"
                ),
            ],
        )
