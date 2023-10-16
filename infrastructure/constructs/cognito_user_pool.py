import base64
import hashlib
import random
import string
from datetime import datetime
import json

import aws_cdk
from aws_cdk import (
    aws_cognito,
    custom_resources,
    Stack,
    triggers,
    aws_lambda,
    aws_iam,
    Duration,
)
from aws_cdk.aws_cognito import UserPoolClient
from aws_cdk.aws_cognito_identitypool_alpha import (
    IdentityPool,
    IdentityPoolAuthenticationProviders,
    UserPoolAuthenticationProvider,
)
from aws_cdk.aws_iam import Policy, PolicyDocument, PolicyStatement
from aws_cdk.aws_logs import RetentionDays
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from aws_cdk.custom_resources import AwsCustomResourcePolicy, PhysicalResourceId
from constructs import Construct


class CognitoUserPool(Construct):
    def __init__(
        self,
        scope: Construct,
        id_: str,
        stage_name: str,
        service_config: dict,
        lambda_common_layer: PythonLayerVersion,
    ) -> None:
        super().__init__(scope, id_)

        # Service info
        service_short_name = service_config["service"]["service_short_name"]
        service_name = service_config["service"]["service_name"]
        self.service_config = service_config

        # Create pre-token lambda
        default_lambda_env = self._create_default_lambda_env(
            service_name=service_name,
            service_short_name=service_short_name,
            stage_name=stage_name,
        )

        # Create cognito UserPool
        self.user_pool = self._create_user_pool(service_short_name, stage_name)

        self._create_admin_user_pool_default_client(
            service_short_name, stage_name, self.user_pool
        )

        self._create_admin_user_pool_web_client(
            service_short_name, stage_name, self.user_pool
        )

        # Create random test user
        test_user_name = f"test{self._generate_random_string(5)}@e2e-test.com"
        test_user_password = f"{self._generate_random_string(24)}"

        # Add test user to user_pool
        test_user = self._add_user_to_user_pool(
            test_user_name, test_user_password, self.user_pool
        )
        test_user.node.add_dependency(self.user_pool)
        enable_test_user_password = self._enable_test_user_password(
            test_user_name, test_user_password, self.user_pool
        )
        enable_test_user_password.node.add_dependency(test_user)

        # Create parameters for test user in Parameter Store
        base_parameter_path = f"/gm/dp/{stage_name}/{service_short_name}/admin/test"
        test_user_password_param_key = f"{base_parameter_path}/test_user_password"
        test_user_password_param = self._add_to_parameter_store(
            "UserPassword", test_user_password_param_key, test_user_password, True
        )
        test_user_password_param.node.add_dependency(enable_test_user_password)
        test_user_password_output_id = f"{stage_name}TestAdminUserPasswordParamPath"
        self._create_cf_output(
            output_id=test_user_password_output_id,
            value=f"{base_parameter_path}/test_user_password",
        )
        test_user_name_param_key = f"{base_parameter_path}/test_user"
        test_user_param = self._add_to_parameter_store(
            "UserNameString", test_user_name_param_key, test_user_name
        )
        test_user_param.node.add_dependency(test_user)
        test_user_output_id = f"{stage_name}TestAdminUserNameParamPath"
        self._create_cf_output(
            output_id=test_user_output_id, value=f"{base_parameter_path}/test_user"
        )

        # Create a post deploy lambda and a Trigger to run it on every deploy
        # The lambda will populate default users to the admin user pool
        self._create_post_deploy_lambda_and_trigger(
            default_lambda_env=default_lambda_env,
            lambda_common_layer=lambda_common_layer,
            post_deploy_lambda_name=f"{stage_name}-{service_short_name}-admin-post-deploy",
            default_users=service_config["default_admin_users"],
            user_pool=self.user_pool,
            user_pool_type="admin_portal",
            user_pool_app_client=self.user_pool_default_app_client,
        )

    def _create_admin_user_pool_default_client(
        self, service_short_name, stage_name, user_pool
    ):
        self.user_pool_default_app_client = self._create_user_pool_default_app_client(
            service_short_name, stage_name, user_pool
        )
        output_id = f"{stage_name}CognitoUserPoolDefaultAppClientId"
        self._create_cf_output(
            output_id=output_id,
            value=f"{self.user_pool_default_app_client.user_pool_client_id}",
        )

    def _create_admin_user_pool_web_client(
        self, service_short_name, stage_name, user_pool
    ):
        self.user_pool_web_app_client = self._create_user_pool_web_app_client(
            service_short_name, stage_name, user_pool
        )
        output_id = f"{stage_name}CognitoUserPoolWebAppClientId"
        self._create_cf_output(
            output_id=output_id,
            value=f"{self.user_pool_web_app_client.user_pool_client_id}",
        )

    def _create_user_pool(self, service_short_name, stage_name):
        user_pool = self._create_cognito_user_pool(service_short_name, stage_name)
        user_pool_output_id = f"{stage_name}CognitoUserPoolId"
        self._create_cf_output(
            output_id=user_pool_output_id, value=f"{user_pool.user_pool_id}"
        )
        self._create_default_tenant_group(stage_name, user_pool)
        self._create_identity_pool(user_pool, stage_name, service_short_name)
        return user_pool

    def _create_default_tenant_group(self, stage_name, user_pool):
        default_tenant_group_name = self._generate_default_tenant_hash(stage_name)
        group_description = f"This group is for the default tenant. The group name is the md5 hash of '{stage_name}/default/tenant' prepended by 'tid:' as a type identifier"
        self._create_default_group(
            user_pool,
            group_name=default_tenant_group_name,
            description=group_description,
        )

    def _create_identity_pool(self, user_pool, stage_name, service_short_name):
        idp = IdentityPool(
            self,
            "id",
            identity_pool_name="Name",
            authentication_providers=IdentityPoolAuthenticationProviders(
                user_pools=[UserPoolAuthenticationProvider(user_pool=user_pool)]
            ),
        )

        # bucket_name = "replace-when-we-know-what-bucket-to-use"
        # role = idp.authenticated_role
        # role.attach_inline_policy(
        #     Policy(
        #         self,
        #         "CognitoAccessBucket",
        #         statements=[
        #             PolicyStatement(
        #                 actions=["s3:ListAllMyBuckets", "s3:GetBucketLocation"],
        #                 resources=["*"],
        #             ),
        #             PolicyStatement(
        #                 actions=["s3:ListBucket"],
        #                 resources=["arn:aws:s3:::bucket-name"],
        #                 conditions={
        #                     "StringLike": {
        #                         "s3:prefix": [
        #                             "",
        #                             "/",
        #                             "${cognito-identity.amazonaws.com:sub}/*",
        #                         ]
        #                     }
        #                 },
        #             ),
        #             PolicyStatement(
        #                 actions=["s3:*"],
        #                 resources=[
        #                     f"arn:aws:s3:::{bucket_name}/${{cognito-identity.amazonaws.com:sub}}",
        #                     f"arn:aws:s3:::{bucket_name}/${{cognito-identity.amazonaws.com:sub}}/*",
        #                 ],
        #             ),
        #         ],
        #     )
        # )

        return idp

    @staticmethod
    def _create_default_lambda_env(service_name, service_short_name, stage_name):
        return {
            "SERVICE_NAME": f"{service_name}",  # for logger, tracer and metrics
            "SERVICE_SHORT_NAME": f"{service_short_name}",  # for logger, tracer and metrics
            "POWERTOOLS_SERVICE_NAME": f"{service_name}",  # for logger, tracer and metrics
            "LOG_LEVEL": "DEBUG",  # for logger
            "STAGE_NAME": stage_name,
        }

    def _attach_user_to_group(
        self,
        stage_name,
        user_pool,
        group_name,
        user_name,
    ):
        return aws_cognito.CfnUserPoolUserToGroupAttachment(
            self,
            f"{stage_name}UserPoolUserToGroupAttachment",
            group_name=group_name,
            username=user_name,
            user_pool_id=user_pool.user_pool_id,
        )

    def _create_default_group(self, user_pool, group_name: str, description: str):
        return aws_cognito.CfnUserPoolGroup(
            self,
            "MyCfnUserPoolGroup",
            user_pool_id=user_pool.user_pool_id,
            group_name=group_name,
            description=description,
        )

    def _add_to_parameter_store(
        self, name, parameter_key, parameter_value, is_secure=False
    ):
        parameter = custom_resources.AwsCustomResource(
            self,
            f"AwsCustomResourceSetAdmin{name}Parameter",
            policy=AwsCustomResourcePolicy.from_sdk_calls(
                resources=AwsCustomResourcePolicy.ANY_RESOURCE
            ),
            on_create=custom_resources.AwsSdkCall(
                service="SSM",
                action="putParameter",
                parameters={
                    "Description": "password for the test user in the cognito userpool",
                    "Name": f"{parameter_key}",
                    "Value": f"{parameter_value}",
                    "Type": "SecureString" if is_secure else "String",
                    "Overwrite": False,
                },
                physical_resource_id=PhysicalResourceId.of(str(datetime.utcnow())),
            ),
            on_update=None,
            on_delete=custom_resources.AwsSdkCall(
                service="SSM",
                action="deleteParameter",
                parameters={
                    "Name": f"{parameter_key}",
                },
            ),
        )
        return parameter

    def _enable_test_user_password(self, test_user_name, test_user_password, user_pool):
        admin_set_user_password = custom_resources.AwsCustomResource(
            self,
            "AwsCustomResourceAdminSetUserPassword",
            policy=AwsCustomResourcePolicy.from_sdk_calls(
                resources=AwsCustomResourcePolicy.ANY_RESOURCE
            ),
            on_create=custom_resources.AwsSdkCall(
                service="CognitoIdentityServiceProvider",
                action="adminSetUserPassword",
                parameters={
                    "UserPoolId": f"{user_pool.user_pool_id}",
                    "Username": f"{test_user_name}",
                    "Password": f"{test_user_password}",
                    "Permanent": True,
                },
                physical_resource_id=PhysicalResourceId.of(
                    f"AwsCustomResource-ForcePassword-{test_user_name}"
                ),
            ),
            on_update=None,
        )
        return admin_set_user_password

    def _add_user_to_user_pool(self, test_user_name, test_user_password, user_pool):
        add_user = custom_resources.AwsCustomResource(
            self,
            "AwsCustomResourceCreateUserPoolUser",
            policy=AwsCustomResourcePolicy.from_sdk_calls(
                resources=AwsCustomResourcePolicy.ANY_RESOURCE
            ),
            on_create=custom_resources.AwsSdkCall(
                service="CognitoIdentityServiceProvider",
                action="adminCreateUser",
                parameters={
                    "UserPoolId": f"{user_pool.user_pool_id}",
                    "Username": f"{test_user_name}",
                    "MessageAction": "SUPPRESS",
                    "TemporaryPassword": f"{test_user_password}",
                },
                physical_resource_id=PhysicalResourceId.of(
                    f"AwsCustomResource-CreateUser-{test_user_name}"
                ),
            ),
        )
        return add_user

    def _create_post_deploy_lambda_and_trigger(
        self,
        default_lambda_env,
        lambda_common_layer,
        post_deploy_lambda_name,
        default_users,
        user_pool,
        user_pool_type,
        user_pool_app_client,
    ):
        post_deploy_lambda_role_name = f"{post_deploy_lambda_name}-role"
        post_deploy_lambda_role = self._build_lambda_role(
            role_name=post_deploy_lambda_role_name
        )
        post_deploy_lambda_env = default_lambda_env.copy()
        post_deploy_lambda_env["USER_POOL_ID"] = user_pool.user_pool_id
        post_deploy_lambda_env["USER_POOL_TYPE"] = user_pool_type
        post_deploy_lambda_env[
            "COGNITO_CLIENT_ID"
        ] = user_pool_app_client.user_pool_client_id

        default_users_as_base64_string = self._users_to_base64(default_users)
        post_deploy_lambda_env["DEFAULT_USERS"] = default_users_as_base64_string
        post_deploy_lambda_env["DEPLOYMENT_TIME"] = datetime.utcnow().isoformat(
            sep="T", timespec="milliseconds"
        )
        post_deploy_lambda = self._build_lambda(
            lambda_name=post_deploy_lambda_name,
            lambda_handler="user_manager.handlers.post_deploy_trigger.invoke",
            lambda_layer=lambda_common_layer,
            lambda_role=post_deploy_lambda_role,
            lambda_env=post_deploy_lambda_env,
        )
        aws_account = Stack.of(self).account
        region = Stack.of(self).region
        policy_statement = aws_iam.PolicyStatement()
        policy_statement.add_actions(
            "cognito-idp:*",
            "ssm:*",
        )
        policy_statement.add_resources(
            f"arn:aws:cognito-idp:{region}:{aws_account}:*",
            f"arn:aws:ssm:{region}:{aws_account}:*",
        )
        post_deploy_lambda.add_to_role_policy(policy_statement)

        triggers.Trigger(
            self,
            f"{post_deploy_lambda_name}Trigger",
            handler=post_deploy_lambda,
            timeout=Duration.minutes(5),
            invocation_type=triggers.InvocationType.EVENT,
        )

        return post_deploy_lambda, post_deploy_lambda_name

    def _build_lambda(
        self: Construct,
        lambda_name: str,
        lambda_handler: str,
        lambda_layer: PythonLayerVersion,
        lambda_role,
        lambda_env: dict,
    ) -> aws_lambda.Function:
        return aws_lambda.Function(
            self,
            lambda_name,
            function_name=lambda_name,
            runtime=aws_lambda.Runtime.PYTHON_3_10,
            code=aws_lambda.Code.from_asset(".build/lambdas/"),
            handler=lambda_handler,
            environment=lambda_env,
            tracing=aws_lambda.Tracing.ACTIVE,
            retry_attempts=0,
            timeout=Duration.seconds(15),
            layers=[lambda_layer],
            role=lambda_role,
            memory_size=128,
            log_retention=RetentionDays.TWO_WEEKS,
        )

    @staticmethod
    def _users_to_base64(users) -> str:
        users_as_json = json.dumps(users)
        users_as_bytes = users_as_json.encode("utf-8")
        user_as_base64_bytes = base64.b64encode(users_as_bytes)
        user_as_base64_string = user_as_base64_bytes.decode("utf-8")
        return user_as_base64_string

    @staticmethod
    def _create_user_pool_default_app_client(
        service_short_name, stage_name, user_pool
    ) -> UserPoolClient:
        app_client = user_pool.add_client(
            f"{stage_name}-{service_short_name}-AdminUserPoolDefaultAppClient",
            user_pool_client_name=f"{stage_name}-{service_short_name}-admin-user-pool-default-app-client",
            auth_flows=aws_cognito.AuthFlow(
                user_password=True,
            ),
        )
        return app_client

    @staticmethod
    def _create_user_pool_web_app_client(
        service_short_name, stage_name, user_pool
    ) -> UserPoolClient:
        app_client = user_pool.add_client(
            f"{stage_name}-{service_short_name}-AdminUserPoolWebAppClient",
            user_pool_client_name=f"{stage_name}-{service_short_name}-admin-user-pool-web-app-client",
            auth_flows=aws_cognito.AuthFlow(
                user_password=True,
            ),
        )
        return app_client

    def _create_cf_output(self, output_id, value):
        aws_cdk.CfnOutput(
            self,
            id=output_id,
            value=value,
            export_name=output_id,
        ).override_logical_id(output_id)

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

    def _create_cognito_user_pool(self, service_short_name, stage_name):
        user_pool = aws_cognito.UserPool(
            self,
            f"{stage_name}-{service_short_name}-CognitoUserPool",
            user_pool_name=f"{stage_name}-{service_short_name}-cognito-user-pool",
            self_sign_up_enabled=True,
            sign_in_case_sensitive=True,
            standard_attributes={
                "email": {
                    "required": True,
                    "mutable": False,
                }
            },
            password_policy=aws_cognito.PasswordPolicy(
                min_length=12,
            ),
            removal_policy=aws_cdk.RemovalPolicy.DESTROY,
        )
        return user_pool

    @staticmethod
    def _generate_random_string(length: int):
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def _generate_default_tenant_hash(stage_name: str):
        plain_text = f"{stage_name}/default/tenant"
        md5_hash_function = hashlib.md5()
        md5_hash_function.update(plain_text.encode())
        hex_hash = md5_hash_function.hexdigest()
        return f"tid:{hex_hash}"
