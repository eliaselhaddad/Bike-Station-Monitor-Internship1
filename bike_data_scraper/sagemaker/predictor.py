import sagemaker
from sagemaker import get_execution_role
from sagemaker.sklearn.model import SKLearnPredictor
from predictor_handler import PredictorHandler

if __name__ == "__main__":
    input_df = PredictorHandler(
        IsOpen=True,
        Long="11.979328",
        Lat="57.673475",
        Year=2023,
        Month=10,
        Day=30,
        Hour=16,
        Temperature=11,
        Humidity=87,
        Wind_Speed=10,
        Precipitation=10,
        Visibility=30000,
        Snowfall=0,
        IsWeekend=0,
    ).create_dataframe()
    endpoint_name = "random-forest-endpoint-1"

    predictor = SKLearnPredictor(
        endpoint_name=endpoint_name, sagemaker_session=sagemaker.Session()
    )

    result = predictor.predict(input_df)
    print(f"number of bikes at location is {result}")
