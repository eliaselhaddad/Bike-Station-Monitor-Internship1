import aws_cdk
import boto3
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


# VIKTIGT: OBJEKT BLIR INSTATIERADE I BÖRJAN AV KODEN
# DET HÄR FÅR DET ATT SE UT SOM ATT EN VARIABEL BLIR KALLAD FÖRE DEN ÄR SKAPAD
# Funktioner i classer kallar för metoder

from constructs import Construct
import os


# Class definition and inheritance
class DataFetchAndSave(Construct):  # inherits from Construct
    def __init__(  # initialize the object and expect the following parameters
        self, scope: Construct, id_: str, stage_name: str, service_config: dict
    ) -> None:
        super().__init__(
            scope, id_
        )  # Calls the parent class' constructor to initialize it

        # extracts the service name from the configuration
        service_short_name = service_config["service"]["service_short_name"]
        service_name = service_short_name

        # Formats the lambda name and role name
        lambda_name = f"{stage_name}-{service_short_name}-data-fetch-and-save"
        lambda_role_name = f"{lambda_name}-role"
        # "lambda_role" calls to get the role object for the lambda
        lambda_role = self._build_lambda_role(lambda_role_name)

        # environment variables for the lambda
        # configure the enviroment differently based on the stage or service
        # this is done by passing in the stage name and service name, which will change based on if you're deploying to dev or prod
        env_vars = self._create_env_vars(
            stage_name=stage_name,
            service_name=service_name,
            service_short_name=service_short_name,
        )

        # builds lambda function - passes in the necessary configuration
        self.data_scraper_lambda = self._build_lambda(
            lambda_name=lambda_name,
            stage_name=stage_name,
            env_vars=env_vars,
            lambda_role=lambda_role,
        )

        # CloudWatch Event to trigger the lambda function every two weeks
        rule = aws_events.Rule(
            self, "Rule", schedule=aws_events.Schedule.rate(Duration.days(14))
        )
        rule.add_target(targets.LambdaFunction(self.data_scraper_lambda))

        # Checks if the S3 bucket exists, if not it creates it
        bucket_name = env_vars["S3_BUCKET_NAME"]
        if self._bucket_exists(bucket_name) is False:
            self._create_s3_bucket(bucket_name)
        else:
            print("Using existing bucket")

    def _bucket_exists(self, bucket_name: str) -> bool:
        s3_client = boto3.client("s3")
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            return True
        except Exception as e:
            print(e)  # This will print the detailed error message
            return False

    def _create_s3_bucket(self, bucket_name: str) -> s3.Bucket:
        unique_id = f"BucketConstruct-{bucket_name}"
        bucket = s3.Bucket(
            self,
            unique_id,
            bucket_name=bucket_name,
            versioned=False,  # bucket versioning is disabled
            encryption=s3.BucketEncryption.S3_MANAGED,  # Server-side encryption with Amazon S3 managed keys (SSE-S3)
            object_ownership=s3.ObjectOwnership.OBJECT_WRITER,  # ACLs disabled (recommended)
            access_control=s3.BucketAccessControl.PUBLIC_READ,
        )

        # Public bucket policy to make it publicly available
        bucket.add_to_resource_policy(
            permission=aws_iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[bucket.bucket_arn, f"{bucket.bucket_arn}/*"],
                effect=aws_iam.Effect.ALLOW,
                principals=[aws_iam.ArnPrincipal("*")],
            )
        )
        return bucket

    # fortsätt med att kolla på hur man kan integrera så att den kollar ifall bucketen redan existerar,
    # så använder den den som redan finns istället för att skapa en ny och deployen funkar som det ska.

    # creates an AWS Lambda function
    def _build_lambda(
        self,
        lambda_name: str,  # name of the lambda
        stage_name: str,  # Sets stage name, so i we know if its dev or prod etc
        env_vars: dict,  # environment variables
        lambda_role: aws_iam.Role,  # Role - for permissions
        cwd: str = os.getcwd(),  # current working directory
    ):
        lambda_function = aws_lambda.Function(
            self,
            lambda_name,  # name that has logicalID in template.json
            function_name=lambda_name,
            runtime=aws_lambda.Runtime.PYTHON_3_10,  # runtime setting - python 3.10
            environment=env_vars,
            code=aws_lambda.Code.from_asset(  # where the code for lambda function exists
                os.path.join(cwd, "bike_data_scraper/handlers")
            ),
            handler="data_fetch_and_save_lambda.lambda_handler",
            tracing=aws_lambda.Tracing.ACTIVE,
            retry_attempts=2,
            timeout=Duration.seconds(80),  # time before lambda times out
            memory_size=128,  # default memory size
            role=lambda_role,
        )
        return lambda_function

    def _build_lambda_role(self, role_name: str) -> aws_iam.Role:
        lambda_role = aws_iam.Role(
            self,
            role_name,
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    managed_policy_name="service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # Adding permissions to access the DynamoDB table
        lambda_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["dynamodb:Scan", "dynamodb:GetItem"],
                resources=[
                    "arn:aws:dynamodb:eu-north-1:796717305864:table/Cyrille-dscrap-bike-data-table"
                ],  # DynamoDB table ARN
            )
        )

        # S3 permissions here
        lambda_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=["arn:aws:s3:::bikedatalake/*"],  # S3 bucket ARN
            )
        )

        return lambda_role

    # behöver lägga till permissions så att lambda funktionen kan läsa från dynamodb och skriva till S3
    # Behöver också schedula ett event med CloudWatch events så det triggas varannan vecka
    def _create_env_vars(
        self, stage_name: str, service_name: str, service_short_name: str
    ) -> dict:
        return {
            "STAGE_NAME": stage_name,
            "S3_BUCKET_NAME": f"{stage_name}-{service_short_name}-processed-bike-data",
            "BIKE_TABLE_NAME": "Cyrille-dscrap-bike-data-table",
            "LOG_LEVEL": "DEBUG",
            "SERVICE_NAME": service_name,
        }

    # def _bucket_exists(self, bucket_name: str) -> bool:
    #     s3_client = boto3.client('s3')
    #     try:
    #         s3_client.head_bucket(Bucket=bucket_name)
    #         return True
    #     except Exception as e:
    #         print(e)
    #         return False

    # if self._bucket_exists(bucket_name):
    #     self.bucket = s3.Bucket.from_bucket_name(self, "ExistingBucket", bucket_name)
    #     bucket_arn = self.bucket.bucket_arn  # Get the ARN for the existing bucket
    #     print("Using existing bucket")
    # else:
    #     self.bucket, bucket_arn = self._create_s3_bucket(bucket_name)

    # use_create_s3_bucket function to create the bucket
