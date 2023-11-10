import joblib


def model_fn(model_dir):
    # Load the model from the model_dir
    model = joblib.load(f"{model_dir}/model.joblib")
    return model
