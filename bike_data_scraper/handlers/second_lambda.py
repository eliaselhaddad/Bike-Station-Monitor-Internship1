def lambda_handler(event, context):
    print("Hello from lambda 2")
    # Save two weeks weather data to .csv

    return {"outputLambda2": "path.to.bucket/weather.csv"}
