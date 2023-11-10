import boto3
import time
from loguru import logger


def lambda_handler(event, context):
    ec2_client = boto3.client("ec2")
    s3_client = boto3.client("s3")

    logger.info("Checking EC2 instance status")
    if "InstanceId" not in event:
        logger.error("InstanceId not found in the event")
        raise ValueError("InstanceId not found in the event")

    instance_id = event["InstanceId"]
    bucket = "cyrille-dscrap-bucket"
    status_key = "status-keys/job-status.txt"

    logger.info(f"Instance ID: {instance_id}")
    response = ec2_client.describe_instances(InstanceIds=[instance_id])
    state = response["Reservations"][0]["Instances"][0]["State"]["Name"]

    logger.info(f"Instance state: {state}")
    if state == "stopped":
        try:
            logger.info("Checking job status")
            s3_client.get_object(Bucket=bucket, Key=status_key)
            job_status = "Completed"
        except s3_client.exceptions.NoSuchKey:
            logger.info("Job status InProgress")
            job_status = "InProgress"
    else:
        logger.info("Job status InProgress")
        job_status = "InProgress"

    time.sleep(30)

    logger.info(f"Instance ID: {instance_id}")
    logger.info(f"Instance JobStatus: {job_status}")
    return {"JobStatus": job_status, "InstanceId": instance_id}
