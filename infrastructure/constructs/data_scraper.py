import aws_cdk
from aws_cdk import aws_dynamodb, aws_lambda, aws_iam, Duration

from constructs import Construct

from aws_cdk.aws_logs import RetentionDays


class DataScraper(Construct):
    def __init__(
        self, scope: Construct, id_: str, stage_name: str, service_config: dict
    ) -> None:
        super().__init__(scope, id_)

        service_name = service_config["service"]["service_name"]
        service_short_name = service_config["service"]["service_short_name"]

        # Dynamodb setup
        dynamodb_name_prefix = f"{stage_name}-{service_short_name}"

        # Device table
        bike_data_table_name = f"{dynamodb_name_prefix}-bike-data-table"
        self.bike_data_table = self._create_dynamodb_table(bike_data_table_name)

        # Lambda setup
        lambda_name = f"{stage_name}-{service_short_name}-data-scraper"
        lambda_role_name = f"{lambda_name}-role"
        self.lambda_role = self._build_lambda_role(role_name=lambda_role_name)
        env_vars = self._create_env_vars(
            stage_name=stage_name,
            service_name=service_name,
        )
        self.data_scraper_lambda = self._build_lambda(
            stage_name=stage_name, lambda_name=lambda_name, env_vars=env_vars
        )
        self.bike_data_table.grant_read_write_data(self.data_scraper_lambda)

    def _create_env_vars(self, stage_name: str, service_name: str):
        return {
            "STAGE_NAME": stage_name,
            "DEVICE_TABLE_NAME": self.bike_data_table.table_name,
            "POWERTOOLS_SERVICE_NAME": f"{service_name}",
            "LOG_LEVEL": "DEBUG",
        }

    def _build_lambda(self, lambda_name: str, stage_name: str, env_vars: dict):
        lambda_function = aws_lambda.Function(
            self,
            lambda_name,
            function_name=lambda_name,
            runtime=aws_lambda.Runtime.NODEJS_18_X,
            environment=env_vars,
            code=aws_lambda.Code.from_asset(".build/lambdas/"),
            handler="bike_data_scraper.handlers.lambda_scrapper.handler",
            tracing=aws_lambda.Tracing.ACTIVE,
            retry_attempts=2,
            timeout=Duration.seconds(15),
            memory_size=128,
            role=self.lambda_role,
            log_retention=RetentionDays.ONE_DAY,
        )
        return lambda_function

    def _create_dynamodb_table(self, table_name: str) -> aws_dynamodb.Table:
        return aws_dynamodb.Table(
            self,
            table_name,
            table_name=table_name,
            partition_key=aws_dynamodb.Attribute(
                name="StationId", type=aws_dynamodb.AttributeType.STRING
            ),
            sort_key=aws_dynamodb.Attribute(
                name="timestamp", type=aws_dynamodb.AttributeType.STRING
            ),
            removal_policy=aws_cdk.RemovalPolicy.DESTROY,
            encryption=aws_dynamodb.TableEncryption.DEFAULT,
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
        )

    def _build_lambda_role(self, role_name: str) -> aws_iam.Role:
        return aws_iam.Role(
            self,
            role_name,
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    managed_policy_name="service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
