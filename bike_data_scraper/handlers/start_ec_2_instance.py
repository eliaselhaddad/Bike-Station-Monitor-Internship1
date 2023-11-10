from loguru import logger
import boto3


def lambda_handler(event, context):
    ec2 = boto3.resource("ec2")
    ssm = boto3.client("ssm")

    training_date = event.get("date")

    try:
        logger.info("Creating EC2 instance")
        instances = ec2.create_instances(
            ImageId="ami-03a2c69daedb78c95",
            MinCount=1,
            MaxCount=1,
            InstanceType="t3.xlarge",
            KeyName="sosTrainKey",
            IamInstanceProfile={"Name": "my-sagemaker-instance-profile"},
            UserData=f"""#!/bin/bash
            yum update -y
            yum groupinstall 'Development Tools' -y
            yum install openssl-devel bzip2-devel libffi-devel -y
            yum install python3 -y

            python3 -m ensurepip

            python3 -m venv /home/ec2-user/env
            source /home/ec2-user/env/bin/activate

            python3 -m pip install --upgrade pip
            python3 -m pip install pandas scikit-learn boto3 joblib sagemaker python-dateutil

            aws s3 cp s3://cyrille-dscrap-bucket/scripts/train.py /home/ec2-user/train.py
            aws s3 cp s3://cyrille-dscrap-bucket/scripts/deploy.py /home/ec2-user/deploy.py
            aws s3 cp s3://sagemaker-eu-north-1-796717305864/sagemaker/sklearncontainer/xtrain2.csv /home/ec2-user/data/xtrain.csv
            aws s3 cp s3://sagemaker-eu-north-1-796717305864/sagemaker/sklearncontainer/xtest2.csv /home/ec2-user/data/xtest.csv
            aws s3 cp s3://sagemaker-eu-north-1-796717305864/sagemaker/sklearncontainer/ytrain2.csv /home/ec2-user/data/ytrain.csv
            aws s3 cp s3://sagemaker-eu-north-1-796717305864/sagemaker/sklearncontainer/ytest2.csv /home/ec2-user/data/ytest.csv

            yourfilenames=`ls /home/ec2-user/data`
            for eachfile in $yourfilenames
            do
                echo $eachfile
            done
            chmod +x /home/ec2-user/train.py
            chmod +x /home/ec2-user/deploy.py

            /home/ec2-user/env/bin/python /home/ec2-user/deploy.py --date {training_date}
            """,
        )

        instance_id = instances[0].id
        logger.info(f"EC2 instance created with ID: {instance_id}")

        param_name = f"/ec2-instance-ids/{instance_id}"
        ssm.put_parameter(
            Name=param_name,
            Description="EC2 instance ID for shutdown",
            Value=instance_id,
            Type="String",
        )
        logger.info(f"Instance ID stored in Parameter Store with name: {param_name}")

        logger.info("EC2 instance created")
        logger.info(f"Instance ID: {instances[0].id}")
        return {"InstanceId": instance_id}
    except Exception as e:
        logger.error(f"Error creating EC2 instance: {e}")
        raise
