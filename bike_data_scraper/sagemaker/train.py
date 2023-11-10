import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib
import logging
import os
from sklearn.model_selection import train_test_split

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    logging.info("Starting training")

    # xtrain_path = os.path.join(os.environ["SM_CHANNEL_TRAINING"], "xtrain2.csv")
    # xtest_path = os.path.join(os.environ["SM_CHANNEL_TRAINING"], "xtest2.csv")
    # ytrain_path = os.path.join(os.environ["SM_CHANNEL_TRAINING"], "ytrain2.csv")
    # ytest_path = os.path.join(os.environ["SM_CHANNEL_TRAINING"], "ytest2.csv")

    # logging.info("Reading data")
    # logging.info(f"Reading data from {xtrain_path}")
    # X_train = pd.read_csv(xtrain_path)

    # logging.info(f"Reading data from {xtest_path}")
    # X_test = pd.read_csv(xtest_path)

    # logging.info(f"Reading data from {ytrain_path}")
    # y_train = pd.read_csv(ytrain_path)

    # logging.info(f"Reading data from {ytest_path}")
    # y_test = pd.read_csv(ytest_path)

    # logging.info("Training model")
    # model = RandomForestRegressor()

    # logging.info("Fitting model")
    # model.fit(X_train, y_train)

    # accuracy = model.score(X_test, y_test)
    # logging.info(f"Accuracy: {accuracy * 100:.2f}%")

    logging.info("Reading data")
    data_path = os.path.join(
        os.environ["SM_CHANNEL_TRAINING"], "StationaryStations.csv"
    )

    logging.info(f"Reading data from {data_path}")
    data = pd.read_csv(data_path)

    logging.info("Training model")
    model = RandomForestRegressor()

    logging.info("Splitting data")

    X_train = data.drop("TotalAvailableBikes", axis=1)
    y_train = data["TotalAvailableBikes"]

    logging.info("Fitting model")
    model.fit(X_train, y_train)
    joblib.dump(model, "/opt/ml/model/model.joblib")
    joblib.dump(model, os.path.join(os.environ["SM_MODEL_DIR"], "model.joblib"))
    logging.info("Model saved to /opt/ml/model/model.joblib")


def model_fn(model_dir):
    """Deserialized and return fitted model
    Note: this should have the same name as the serialized model in the main method
    """
    model = joblib.load(f"{model_dir}/model.joblib")
    return model
