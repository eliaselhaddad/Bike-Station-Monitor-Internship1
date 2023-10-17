def lambda_handler(event, context):
    print("Hello from finishing lambda (third_lambda.py")
    # Do Data pre processing lambda
    # Save to Csv for machine learning and creating Graphs
    # Now I know which .csvs to use
    print(f"Printing event received from the other two lambdas: {event}")
    return {"path": "toDataPrepared.csv"}
