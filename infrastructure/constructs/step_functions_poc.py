import aws_cdk
from aws_cdk import (
    aws_dynamodb,
    aws_lambda,
    aws_iam,
    Duration,
    aws_events,
    aws_events_targets,
)
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion

from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda as lambda_,
)

from aws_cdk.aws_logs import RetentionDays
import os


class StepFunctionsPoc(Construct):
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
        service_name = service_config["service"]["service_name"]
        service_short_name = service_config["service"]["service_short_name"]

        weather_lambda = aws_lambda.Function.from_function_name(
            self,
            id=f"{stage_name}-{service_short_name}-weather-data-scraper",
            function_name=f"{stage_name}-{service_short_name}-weather-data-scraper",
        )

        bike_lambda = aws_lambda.Function.from_function_name(
            self,
            id=f"{stage_name}-{service_short_name}-data-scraper",
            function_name=f"{stage_name}-{service_short_name}-data-scraper",
        )

        fetch_from_db_lambda = aws_lambda.Function.from_function_name(
            self,
            id=f"{stage_name}-{service_short_name}-save-two-weeks-bike-data-to-csv",
            function_name=f"{stage_name}-{service_short_name}-save-two-weeks-bike-data-to-csv",
        )

        processed_data_lambda = aws_lambda.Function.from_function_name(
            self,
            id=f"{stage_name}-{service_short_name}-data-preprocessed",
            function_name=f"{stage_name}-{service_short_name}-data-preprocessed",
        )

        graphs_lambda = aws_lambda.Function.from_function_name(
            self,
            id=f"{stage_name}-{service_short_name}-graphs-data-scraper",
            function_name=f"{stage_name}-{service_short_name}-graphs-data-scraper",
        )

        lambda_role_name = f"{weather_lambda}-role"
        self.lambda_role = self._build_lambda_role(role_name=lambda_role_name)

        first_parallel_lambda_task = tasks.LambdaInvoke(
            self, id="FetchBikeDataFromApi", lambda_function=bike_lambda
        )
        second_parallel_lambda_task = tasks.LambdaInvoke(
            self,
            id="FetchWeatherDataFromApi",
            lambda_function=weather_lambda,
        )
        third_lambda_task = tasks.LambdaInvoke(
            self, id="SaveBikeDataToS3", lambda_function=fetch_from_db_lambda
        )
        fourth_lambda_task = tasks.LambdaInvoke(
            self, id="ProcessAndMergeDatsets", lambda_function=processed_data_lambda
        )
        fifth_lambda_task = tasks.LambdaInvoke(
            self, id="FetchGraphsData", lambda_function=graphs_lambda
        )

        parallel_task = (
            sfn.Parallel(self, "FetchWeatherAndBikeData")
            .branch(first_parallel_lambda_task)
            .branch(second_parallel_lambda_task)
        )
        parallel_task.next(third_lambda_task)
        third_lambda_task.next(fourth_lambda_task)
        fourth_lambda_task.next(fifth_lambda_task)

        sfn.StateMachine(self, id="MyStepMachinesId", definition=parallel_task)

    def _build_lambda(
        self,
        lambda_name: str,
        stage_name: str,
        lambda_path: str,
        cwd: str = os.getcwd(),
    ):
        lambda_function = aws_lambda.Function(
            self,
            lambda_name,
            function_name=lambda_name,
            runtime=aws_lambda.Runtime.PYTHON_3_10,
            code=aws_lambda.Code.from_asset(os.path.join(cwd, ".build/lambdas/")),
            handler=lambda_path,
            tracing=aws_lambda.Tracing.ACTIVE,
            retry_attempts=2,
            timeout=Duration.seconds(80),
            memory_size=128,
            role=self.lambda_role,
            layers=[self.lambda_layer],
        )
        return lambda_function

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
