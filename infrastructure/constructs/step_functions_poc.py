import aws_cdk
from aws_cdk import (
    aws_lambda,
    aws_events,
    aws_events_targets,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion

from constructs import Construct

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

        self.account = aws_cdk.Stack.of(self).account
        self.region = aws_cdk.Stack.of(self).region

        self.lambda_layer = lambda_layer
        service_name = service_config["service"]["service_name"]
        service_short_name = service_config["service"]["service_short_name"]

        self.weather_lambda = aws_lambda.Function.from_function_name(
            self,
            id=f"{stage_name}-{service_short_name}-weather-data-scraper",
            function_name=f"{stage_name}-{service_short_name}-weather-data-scraper",
        )

        self.bike_lambda = aws_lambda.Function.from_function_name(
            self,
            id=f"{stage_name}-{service_short_name}-data-scraper",
            function_name=f"{stage_name}-{service_short_name}-data-scraper",
        )

        self.fetch_from_db_lambda = aws_lambda.Function.from_function_name(
            self,
            id=f"{stage_name}-{service_short_name}-save-two-weeks-bike-data-to-csv",
            function_name=f"{stage_name}-{service_short_name}-save-two-weeks-bike-data-to-csv",
        )

        self.processed_data_lambda = aws_lambda.Function.from_function_name(
            self,
            id=f"{stage_name}-{service_short_name}-data-preprocessed",
            function_name=f"{stage_name}-{service_short_name}-data-preprocessed",
        )

        self.graphs_lambda = aws_lambda.Function.from_function_name(
            self,
            id=f"{stage_name}-{service_short_name}-graphs-data-scraper",
            function_name=f"{stage_name}-{service_short_name}-graphs-data-scraper",
        )

        first_parallel_lambda_task = tasks.LambdaInvoke(
            self, id="FetchBikeDataFromApi", lambda_function=self.bike_lambda
        )

        second_parallel_lambda_task = tasks.LambdaInvoke(
            self,
            id="FetchWeatherDataFromApi",
            lambda_function=self.weather_lambda,
        )
        third_lambda_task = tasks.LambdaInvoke(
            self, id="SaveBikeDataToS3", lambda_function=self.fetch_from_db_lambda
        )
        fourth_lambda_task = tasks.LambdaInvoke(
            self,
            id="ProcessAndMergeDatsets",
            lambda_function=self.processed_data_lambda,
        )
        fifth_lambda_task = tasks.LambdaInvoke(
            self, id="FetchGraphsData", lambda_function=self.graphs_lambda
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

        self.cron_job_eventbride_rule = self._cron_job_eventbride_rule(
            state_machine_name="MyStepMachinesId",
            cron_expression="cron(0 12 ? * MON#3 *)",
        )

    def _cron_job_eventbride_rule(self, state_machine_name: str, cron_expression: str):
        rule = aws_events.Rule(
            self,
            f"{state_machine_name}-cron-job-eventbridge-rule",
            rule_name=f"{state_machine_name}-cron-job-eventbridge-rule",
            enabled=True,
            schedule=aws_events.Schedule.expression(expression=cron_expression),
        )
        state_machine = sfn.StateMachine.from_state_machine_arn(
            self,
            "ExistingStateMachine",
            state_machine_arn=f"arn:aws:states:{self.region}:{self.account}:stateMachine:{state_machine_name}",
        )
        rule.add_target(aws_events_targets.SfnStateMachine(state_machine))
        return rule
