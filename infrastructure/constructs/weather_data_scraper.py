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
            stage_name=stage_name,
            service_name=service_name,
            service_short_name=service_short_name,
        )

        # Create the S3 bucket using the environment variable for the bucket name
        self.weather_data_bucket = self._create_s3_bucket(env_vars["S3_BUCKET_NAME"])

        self.data_scraper_lambda = self._build_lambda(
            lambda_name=lambda_name,
            stage_name=stage_name,
            env_vars=env_vars,
            lambda_role=lambda_role,
        )

        # Eventbridge setup for a cron job
        self.cron_job_eventbride_rule = self._cron_job_eventbride_rule(
            lambda_name=lambda_name,
            cron_expression="cron(0 12 ? * MON#3 *)",  # Every two weeks on Monday at 12:00 PM
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

    def _create_env_vars(
        self, stage_name: str, service_name: str, service_short_name: str
    ):
        return {
            "STAGE_NAME": stage_name,
            "S3_BUCKET_NAME": f"{stage_name}-{service_short_name}-raw-weather-data",
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

    def _create_s3_bucket(self, bucket_name: str) -> aws_s3.Bucket:
        return aws_s3.Bucket(
            self,
            "WeatherDataBucket",
            bucket_name=bucket_name,
            public_read_access=True,
            versioned=True,
        )

    def _cron_job_eventbride_rule(self, lambda_name: str, cron_expression: str):
        rule = aws_events.Rule(
            self,
            f"{lambda_name}-cron-job-eventbride-rule",
            rule_name=f"{lambda_name}-cron-job-eventbride-rule",
            enabled=True,
            schedule=aws_events.Schedule.expression(expression=cron_expression),
        )
        rule.add_target(aws_events_targets.LambdaFunction(self.data_scraper_lambda))
        return rule
