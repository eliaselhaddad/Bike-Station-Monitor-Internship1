def lambda_handler(event, context):
    print("Hello from parallel lambda 1")
    # Save two weeks bike data to .csv
    return {"outputLambda1": "sath.to.bucket/bike.csv"}
