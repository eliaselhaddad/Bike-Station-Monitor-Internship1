import builtins
import aws_cdk
from aws_cdk import (
    aws_dynamodb,
    aws_lambda,
    aws_iam,
    Duration,
    aws_events,
    aws_events_targets,
    aws_s3,
)

from constructs import Construct
import os


class GraphsDataScraper(Construct):
    def __init__(
        self,
        scope: Construct,
        id_: str,
        stage_name: str,
        service_config: dict,
        lambda_layer: aws_lambda.LayerVersion,
    ) -> None:
        super().__init__(scope, id_)

        service_short_name = service_config["service"]["service_short_name"]
        service_name = service_short_name
        cwd = os.getcwd()

        self.lambda_layer = lambda_layer
        s3_name_prefix = f"{stage_name}-{service_short_name}"
        # self.s3_bucket = self._create_s3_bucket(self, s3_name_prefix)

        # lambda setup
        lambda_name = f"{stage_name}-{service_short_name}-graphs-data-scraper"
        lambda_role_name = f"{lambda_name}-role"
        lambda_role = self._build_lambda_role(lambda_role_name)
        env_vars = self._create_env_vars(
            stage_name=stage_name,
            service_name=service_name,
            s3_destination_bucket=f"{stage_name}-{service_short_name}-cyrille-dscrap-bucket",
            s3_source_bucket=f"{stage_name}-{service_short_name}-cyrille-dscrap-bucket/processed",
        )

        self.data_scraper_lambda = self._build_lambda(
            lambda_name=lambda_name,
            stage_name=stage_name,
            env_vars=env_vars,
            lambda_role=lambda_role,
        )

    def _create_env_vars(
        self,
        stage_name: str,
        service_name: str,
        s3_source_bucket: str,
        s3_destination_bucket: str,
    ):
        return {
            "STAGE_NAME": stage_name,
            "SERVICE_NAME": service_name,
            "GRAPHS_SOURCE_BUCKET": s3_source_bucket,
            "GRAPHS_DESTINATION_BUCKET": s3_destination_bucket,
            "STATION_BIKE_DATA": "station_bikes/2023-09-10-2023-09-24",
            "SINGLE_BIKE_DATA": "single_bikes/2023-09-10-2023-09-24",
            "LOG_LEVEL": "DEBUG",
        }

    # def _create_s3_bucket(self, scope: Construct, s3_name_prefix: str) -> aws_s3.Bucket:
    #     return aws_s3.Bucket(
    #         scope,
    #         f"{s3_name_prefix}-bucket",
    #         bucket_name=f"{s3_name_prefix}-bucket".lower(),
    #         removal_policy=aws_cdk.RemovalPolicy.DESTROY,
    #         auto_delete_objects=True,
    #     )

    def _build_lambda(
        self,
        lambda_name: str,
        stage_name: str,
        env_vars: dict,
        lambda_role: aws_iam.Role,
        cwd: str = os.getcwd(),
    ):
        lambda_function = aws_lambda.Function(
            self,
            lambda_name,
            function_name=lambda_name,
            runtime=aws_lambda.Runtime.PYTHON_3_10,
            environment=env_vars,
            code=aws_lambda.Code.from_asset(os.path.join(cwd, ".build/lambdas/")),
            handler="bike_data_scraper/handlers/graphs_data_scraper.lambda_handler",
            tracing=aws_lambda.Tracing.ACTIVE,
            retry_attempts=2,
            timeout=Duration.seconds(900),
            role=lambda_role,
        )

        return lambda_function

    def _build_lambda_role(self, role_name: str) -> aws_iam.Role:
        return aws_iam.Role(
            self,
            role_name,
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )
