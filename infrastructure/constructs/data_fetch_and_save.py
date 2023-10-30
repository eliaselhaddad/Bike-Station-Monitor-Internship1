import aws_cdk
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from botocore.exceptions import ClientError
from aws_cdk import (
    aws_dynamodb,
    aws_lambda,
    aws_iam,
    Duration,
    aws_events,
    aws_events_targets as targets,
    aws_s3 as s3,
)
from constructs import Construct
import os


class SaveTwoWeeksBikeDataToCsv(Construct):
    def __init__(
        self,
        scope: Construct,
        id_: str,
        stage_name: str,
        service_config: dict,
        lambda_layer: PythonLayerVersion,
    ) -> None:
        super().__init__(scope, id_)
        self.lambda_layer = lambda_layer
        service_short_name = service_config["service"]["service_short_name"]
        service_name = service_short_name

        lambda_name = (
            f"{stage_name}-{service_short_name}-save-two-weeks-bike-data-to-csv"
        )
        lambda_role_name = f"{lambda_name}-role"
        lambda_role = self._build_lambda_role(lambda_role_name)

        env_vars = self._create_env_vars(
            stage_name=stage_name,
            service_name=service_name,
            service_short_name=service_short_name,
        )

        self.data_fetch_and_save_lambda = self._build_lambda(
            lambda_name=lambda_name,
            stage_name=stage_name,
            env_vars=env_vars,
            lambda_role=lambda_role,
        )

        rule = aws_events.Rule(
            self, "Rule", schedule=aws_events.Schedule.rate(Duration.days(14))
        )
        rule.add_target(targets.LambdaFunction(self.data_fetch_and_save_lambda))

        lambda_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["dynamodb:Scan", "dynamodb:GetItem"],
                resources=[
                    f"arn:aws:dynamodb:eu-north-1:796717305864:table/{stage_name}-{service_short_name}-bike-data-table"
                ],
            )
        )

        lambda_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[
                    f"arn:aws:s3:::{stage_name}-{service_short_name}-raw-weather-data"
                ],
            )
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
            code=aws_lambda.Code.from_asset(os.path.join(cwd, ".build/lambdas")),
            handler="bike_data_scraper/handlers/data_fetch_and_save_lambda.lambda_handler",
            tracing=aws_lambda.Tracing.ACTIVE,
            retry_attempts=2,
            timeout=Duration.seconds(900),
            memory_size=1024,
            role=lambda_role,
            layers=[self.lambda_layer],
        )
        return lambda_function

    def _build_lambda_role(self, role_name: str) -> aws_iam.Role:
        lambda_role = aws_iam.Role(
            self,
            role_name,
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3FullAccess"
                ),
            ],
        )
        return lambda_role

    def _create_env_vars(
        self, stage_name: str, service_name: str, service_short_name: str
    ) -> dict:
        return {
            "STAGE_NAME": stage_name,
            "S3_BUCKET_NAME": f"{stage_name}-{service_short_name}-raw-weather-data",
            "BIKE_TABLE_NAME": f"{stage_name}-{service_short_name}-bike-data-table",
            "LOG_LEVEL": "DEBUG",
            "SERVICE_NAME": service_name,
        }
