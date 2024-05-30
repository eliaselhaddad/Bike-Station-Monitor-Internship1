import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib
import logging
import os

role = "arn:aws:iam::796717305864:role/bike-scrapper-sagemaker-role"
training_job = os.path.join(
    os.getcwd(), "bike_data_scraper", "sagemaker", "training_job.py"
)

if __name__ == "__main__":
    logging.info("Starting training")

    logging.info("Reading data")
    data_path = os.path.join(
        os.environ["SM_CHANNEL_TRAINING"], "StationaryStations.csv"
    )

    logging.info(f"Reading data from {data_path}")
    data = pd.read_csv(data_path)

    logging.info("Training model")
    model = RandomForestRegressor()

    logging.info("Splitting data")

    X, y = (
        data[
            [
                "IsOpen",
                "Long",
                "Lat",
                "Year",
                "Month",
                "Day",
                "Hour",
                "Minute",
                "Temperature",
                "Humidity",
                "Wind_Speed",
                "Precipitation",
                "Visibility",
                "Snowfall",
                "IsWeekend",
            ]
        ],
        data["TotalAvailableBikes"],
    )

    logging.info("Fitting model")
    model.fit(X, y)
    joblib.dump(model, "/opt/ml/model/model.joblib")
    joblib.dump(model, os.path.join(os.environ["SM_MODEL_DIR"], "model.joblib"))
    logging.info("Model saved to /opt/ml/model/model.joblib")


def model_fn(model_dir):
    """Deserialized and return fitted model
    Note: this should have the same name as the serialized model in the main method
    """
    model = joblib.load(f"{model_dir}/model.joblib")
    return model
