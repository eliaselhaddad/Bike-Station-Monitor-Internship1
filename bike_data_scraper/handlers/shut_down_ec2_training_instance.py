import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    ec2_client = boto3.client("ec2")
    ssm = boto3.client("ssm")

    instance_id = event.get("InstanceId")
    if not instance_id:
        param_name = event["ParamName"]
        instance_id = ssm.get_parameter(Name=param_name)["Parameter"]["Value"]

    logger.info(f"Stopping EC2 instance {instance_id}")
    ec2_client.stop_instances(InstanceIds=[instance_id])

    ssm.delete_parameter(Name=param_name)

    logger.info(f"Instance ID: {instance_id}")
    logger.info(f"Instance with instance_id {instance_id} stopped")
    return {"InstanceId": instance_id, "Message": "Instance stopped"}
