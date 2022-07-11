import pickle
from flask import Flask, request, jsonify
import mlflow
import os
# from dotenv import find_dotenv, load_dotenv

# load_dotenv(find_dotenv())

# using the direct s3 location will work even if tracking server goes down
EXP_ID = 3
# RUN_ID = "72f8ea3dcf6548789a08b95f3a6d3375"
RUN_ID = os.getenv("RUN_ID")
logged_model = f"s3://agifford-mlflow-artifacts-remote/{EXP_ID}/{RUN_ID}/artifacts/model"

# using tracking uri is problematic if server goes down for any reason...
# MLFLOW_TRACKING_URI = f"http://127.0.0.1:5000"
# mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# logged_model = f'runs:/{RUN_ID}/model'
# Load model as a PyFuncModel.
model = mlflow.pyfunc.load_model(logged_model)

# don't need load_dotenv(find_dotenv()) when running this code on the server...
# MLFLOW_TRACKING_URI = f"http://{TRACKING_SERVER_HOST}:5000"

# DON'T NEED TO LOAD THE DICTVECTORIZER AS IN THE TUTORIAL VIDEO BECAUSE MY MODEL IS 
# SAVED AS A PIPELINE WITH THE DICTVECTORIZER AND RF REGRESSOR
# TRACKING_SERVER_HOST = os.environ.get("TRACKING_SERVER_HOST")
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
# path = client.download_artifacts(run_id=RUN_ID, path='dict_vectorizer.bin')
# print(f"downloading the dict vectorizer to {path}")

# with open(path, "rb") as f_in:
#     dv = pickle.load(f_in)


def prepare_features(ride):
    features = {}
    features["PU_DO"] = "%s_%s" % (ride["PULocationID"], ride["DOLocationID"])
    features["trip_distance"] = ride["trip_distance"]
    return features

def predict(features):
    X = dv.transform(features)
    preds = model.predict(X)
    # below ends up working without float(), but keeping it here in case numpy types
    # cause error with Flask and serving
    return float(preds[0])

def pipeline_predict(X):
    preds = model.predict(X)
    # below ends up working without float(), but keeping it here in case numpy types
    # cause error with Flask and serving
    return float(preds[0])

app = Flask("duration-prediction")


@app.route("/predict", methods=["POST"])
def predict_endpoint():
    ride = request.get_json()
    
    features = prepare_features(ride)
    # pred = predict(features)
    pred = pipeline_predict(features)

    result = {
        "duration": pred,
        "model_version": RUN_ID
    }
    return jsonify(result)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=9696)