import argparse
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib
import boto3
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO)


def read_from_s3(bucket_name, file_name):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--bucket-name", type=str, required=True)
    parser.add_argument("--xtrain-key", type=str, required=True)
    parser.add_argument("--xtest-key", type=str, required=True)
    parser.add_argument("--ytrain-key", type=str, required=True)
    parser.add_argument("--ytest-key", type=str, required=True)

    args = parser.parse_args()

    X_train = read_from_s3(args.bucket_name, args.xtrain_key)
    X_test = read_from_s3(args.bucket_name, args.xtest_key)
    y_train = read_from_s3(args.bucket_name, args.ytrain_key)
    y_test = read_from_s3(args.bucket_name, args.ytest_key)

    model = RandomForestRegressor()

    model.fit(X_train, y_train)

    accuracy = model.score(X_test, y_test)
    logging.info(f"Accuracy: {accuracy * 100:.2f}%")

    joblib.dump(model, "/opt/ml/model/model.joblib")
    logging.info("Model saved to /opt/ml/model/model.joblib")


def model_fn(model_dir):
    """Deserialized and return fitted model

    Note: this should have the same name as the serialized model in the main method
    """
    model = joblib.load(f"{model_dir}/model.joblib")
    return model
