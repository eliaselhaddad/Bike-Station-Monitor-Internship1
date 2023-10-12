import aws_cdk
from aws_cdk import (
    aws_dynamodb,
    aws_lambda,
    aws_iam,
    Duration,
    aws_events,
    aws_events_targets,
)

from constructs import Construct

from aws_cdk.aws_logs import RetentionDays
import os


class WeatherDataScraper(Construct):
    def __init__(
        self, scope: Construct, id_: str, stage_name: str, service_config: dict
    ) -> None:
        super().__init__(scope, id_)

        service_short_name = service_config["service"]["service_short_name"]
        service_name = service_short_name

        lambda_name = f"{stage_name}-{service_short_name}-weather-data-scraper"
        lambda_role_name = f"{lambda_name}-role"
        lambda_role = self._build_lambda_role(lambda_role_name)

        env_vars = self._create_env_vars(
            stage_name=stage_name, service_name=service_name
        )

        self.data_scraper_lambda = self._build_lambda(
            lambda_name=lambda_name,
            stage_name=stage_name,
            env_vars=env_vars,
            lambda_role=lambda_role,
        )

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
            code=aws_lambda.Code.from_asset(os.path.join(cwd, "data_scraper/handlers")),
            handler="weather_data_handler.lambda_handler",
            tracing=aws_lambda.Tracing.ACTIVE,
            retry_attempts=2,
            timeout=Duration.seconds(80),
            memory_size=128,
            role=lambda_role,
        )
        return lambda_function

    def _create_env_vars(self, stage_name: str, service_name: str):
        return {
            "STAGE_NAME": stage_name,
            "S3_BUCKET_NAME": "raw-data-weather-and-bikes",
            "LOG_LEVEL": "DEBUG",
            "SERVICE_NAME": service_name,
        }

    def _build_lambda_role(self, role_name: str) -> aws_iam.Role:
        return aws_iam.Role(
            self,
            role_name,
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    managed_policy_name="service-role/AWSLambdaBasicExecutionRole"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    managed_policy_name="AmazonS3FullAccess"
                ),
            ],
        )
