from aws_cdk import Environment, Stack
from aws_cdk.aws_codecommit import Repository
from aws_cdk.pipelines import CodePipeline, CodePipelineSource, ShellStep
from constructs import Construct
from infrastructure.data_scraper_stage import DataScraperStage


class DataScraperPipelineStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        repository_arn: str,
        service_config: dict,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        repository = Repository.from_repository_arn(
            self,
            "DataScraperRepo",
            repository_arn=repository_arn,
        )
        pipeline = CodePipeline(
            self,
            "DataScraperPipeline",
            pipeline_name="DataScraperPipeline",
            docker_enabled_for_synth=True,
            cross_account_keys=True,
            synth=ShellStep(
                "Synth",
                input=CodePipelineSource.code_commit(repository, "master"),
                commands=[
                    "npm install -g aws-cdk",
                    "make pipeline_build",
                    "cdk synth",
                ],
            ),
        )
        stage_name = "demo"
        demo_env = Environment(
            account=service_config["stages"][stage_name]["account"],
            region=service_config["stages"][stage_name]["region"],
        )
        stage = DataScraperStage(
            self,
            f"Stage-{stage_name}-DataScraper",
            stage_name=stage_name,
            env=demo_env,
            service_config=service_config,
        )
        pipeline.add_stage(stage)
