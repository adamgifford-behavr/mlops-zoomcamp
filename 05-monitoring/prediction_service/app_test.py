import pickle
import os
from flask import Flask, request, jsonify
import requests
from pymongo import MongoClient

MODEL_FILE = os.getenv('MODEL_FILE', 'lin_reg.bin')
EVIDENTLY_SERVICE_ADDRESS = os.getenv('EVIDENTLY_SERVICE', 'http://127.0.0.1:5000')
MONGODB_ADDRESS = os.getenv("MONGODB_ADDRESS", "mongodb://127.0.0.1:27017")

with open(MODEL_FILE, 'rb') as f_in:
    dv, model = pickle.load(f_in)

app = Flask("duration")
mongo_client = MongoClient(MONGODB_ADDRESS)
db = mongo_client.get_database("mlops_prediction_service")
collection = db.get_collection("data")


def _prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features

def _predict(features):
    X = dv.transform(features)
    preds = model.predict(X)
    return float(preds[0])

def _save_to_db(record, prediction):
    rec = record.copy()
    rec["prediction"] = prediction
    collection.insert_one(rec)

def _send_to_evidently_service(record, prediction):
    rec = record.copy()
    rec["prediction"] = prediction
    #arbitrary, just how it is defined in app.py
    requests.post(f"{EVIDENTLY_SERVICE_ADDRESS}/iterate/taxi", json=[rec])


@app.route("/predict", methods=["POST"])
def predict_endpoint():
    ride = request.get_json()

    features = _prepare_features(ride)
    pred = _predict(features)

    result = {
        'duration': pred
    }

    _save_to_db(record, pred)
    _send_to_evidently_service(record, pred)

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9696)