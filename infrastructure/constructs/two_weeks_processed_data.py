import aws_cdk
from aws_cdk import (
    aws_lambda,
    aws_iam,
    Duration,
    aws_events,
    aws_events_targets,
    aws_s3,
)

from constructs import Construct

from aws_cdk.aws_logs import RetentionDays
import os


class DataPreprocessed(Construct):
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
        self.s3_bucket = self._create_s3_bucket(self, s3_name_prefix)

        # Lambda setup
        lambda_name = f"{stage_name}-{service_short_name}-data-preprocessed"
        lambda_role_name = f"{lambda_name}-role"
        self.lambda_role = self._build_lambda_role(role_name=lambda_role_name)
        env_vars = self._create_env_vars(
            stage_name=stage_name,
            service_name=service_name,
            s3_destination_bucket=self.s3_bucket.bucket_name,
            s3_source_bucket=f"raw-data-weather-and-bikes",
        )

        # Build Lambda
        self.data_preprocessed_lambda = self._build_lambda(
            stage_name=stage_name, lambda_name=lambda_name, env_vars=env_vars, cwd=cwd
        )

        self.s3_bucket.grant_read_write(self.data_preprocessed_lambda)

    @staticmethod
    def _create_env_vars(
        stage_name: str,
        service_name: str,
        s3_source_bucket: str,
        s3_destination_bucket: str,
    ) -> dict:
        return {
            "STAGE_NAME": stage_name,
            "S3_SOURCE_BUCKET": s3_source_bucket,
            "S3_DESTINATION_BUCKET": s3_destination_bucket,
            "POWERTOOLS_SERVICE_NAME": f"{service_name}",
            "LOG_LEVEL": "DEBUG",
            "SERVICE_NAME": service_name,
            "WEATHER_KEY": "weather-09-29-10-13.csv",
            "BIKES_KEY": "bikes-09-29-10-13.csv",
        }

    def _create_s3_bucket(self, scope: Construct, s3_name_prefix: str) -> aws_s3.Bucket:
        return aws_s3.Bucket(
            scope,
            id=f"{s3_name_prefix}-bucket",
            bucket_name=f"{s3_name_prefix}-bucket".lower(),
            removal_policy=aws_cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

    def _build_lambda_role(self, role_name: str) -> aws_iam.Role:
        role = aws_iam.Role(
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
            ],
        )
        return role

    def _build_lambda(
        self, stage_name: str, lambda_name: str, env_vars: dict, cwd: str
    ) -> aws_lambda.Function:
        return aws_lambda.Function(
            self,
            id=lambda_name,
            function_name=lambda_name,
            runtime=aws_lambda.Runtime.PYTHON_3_10,
            code=aws_lambda.Code.from_asset(path=os.path.join(cwd, ".build/lambdas/")),
            handler="bike_data_scraper/handlers/data_preprocessing_2weeks.lambda_handler",
            tracing=aws_lambda.Tracing.ACTIVE,
            retry_attempts=2,
            role=self.lambda_role,
            environment=env_vars,
            timeout=Duration.seconds(900),
            log_retention=RetentionDays.ONE_WEEK,
            memory_size=1024,
            layers=[self.lambda_layer],
        )
