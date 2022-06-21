import json
import base64
import boto3
import os
import mlflow

PREDICTIONS_STREAM_NAME = os.getenv("PREDICTIONS_STREAM_NAME", "ride_predictions")
TEST_RUN = os.getenv("TEST_RUN", "False") == "True"

kinesis_client = boto3.client("kinesis")

EXP_ID = 3
RUN_ID = os.getenv("RUN_ID")
logged_model = f"s3://agifford-mlflow-artifacts-remote/{EXP_ID}/{RUN_ID}/artifacts/model"
# Load model as a PyFuncModel.
model = mlflow.pyfunc.load_model(logged_model)

def prepare_features(ride):
    features = {}
    features["PU_DO"] = "%s_%s" % (ride["PULocationID"], ride["DOLocationID"])
    features["trip_distance"] = ride["trip_distance"]
    return features
    
def predict(features):
    pred = model.predict(features)
    return float(pred[0])

def lambda_handler(event, context):

    prediction_events = []
    for record in event["Records"]:
        data_encoded = record["kinesis"]["data"]
        data_decoded = base64.b64decode(data_encoded).decode('utf-8')
        ride_event = json.loads(data_decoded)
        ride, ride_id = ride_event["ride"], ride_event["ride_id"]
        
        features = prepare_features(ride)
        prediction = predict(features)
        
        prediction_event = {
            "mode": "ride_prediction_model",
            "version": "123",
            "prediction": {
                'ride_duration': prediction,
                "ride_id": ride_id
            }
        }
        
        if not TEST_RUN:
            kinesis_client.put_record(
                StreamName=PREDICTIONS_STREAM_NAME,
                Data=json.dumps(prediction_event),
                PartitionKey=str(ride_id),
                # ExplicitHashKey=[],  # optional
                # SequenceNumberForOrdering=[]  # optional
            )
        prediction_events.append(prediction_event)
        
    
    # this is now just for testing so we know it's working
    return {
        "predictions": prediction_events
    }
