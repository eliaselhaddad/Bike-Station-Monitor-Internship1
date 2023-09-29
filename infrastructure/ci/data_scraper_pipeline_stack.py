from aws_cdk import Environment, Stack
from aws_cdk.aws_codecommit import Repository
from aws_cdk.pipelines import CodePipeline, CodePipelineSource, ShellStep
from constructs import Construct
from infrastructure.data_scraper_stage import DataScraperStage


class DeviceManagerPipelineStack(Stack):
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
            repository_arn="arn:aws:codecommit:eu-north-1:796717305864:data-scraper",
        )
        pipeline = CodePipeline(
            self,
            "DataScraperPipeline",
            pipeline_name="DataScraperPipeline",
            docker_enabled_for_synth=True,
            cross_account_keys=True,
            synth=ShellStep(
                "Synth",
                input=CodePipelineSource.code_commit(repository, "main"),
                commands=[
                    "npm install -g aws-cdk",
                    "make pipeline_build",
                    "cdk synth",
                ],
            ),
        )
        stage_name = "demo"
        demo_env = Environment(
            account="796717305864",
            region="eu-north-1",
        )
        stage = DataScraperStage(
            self,
            f"Stage-{stage_name}-DataScraper",
            stage_name=stage_name,
            env=demo_env,
            service_config=service_config,
        )
        pipeline.add_stage(stage)
