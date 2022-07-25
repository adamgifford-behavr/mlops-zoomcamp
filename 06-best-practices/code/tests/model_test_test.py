from pathlib import Path

import model_test


def read_text(file):
    test_directory = Path(__file__).parent

    with open(test_directory / file, 'rt', encoding="utf-8") as f_in:
        return f_in.read().strip()

def test_base64_decode():
    base64_input = read_text("data_test.b64")
    actual_result = model_test.base64_decode(base64_input)
    expected_result = {
        "ride": {
            "PULocationID": 130,
            "DOLocationID": 205,
            "trip_distance": 3.66
        }, 
        "ride_id": 256
    }
    assert actual_result == expected_result


def test_prepare_features():
    model_service = model_test.ModelService(None)

    ride = {
        "PULocationID": 130,
        "DOLocationID": 205,
        "trip_distance": 3.66
    }
    
    actual_features = model_service.prepare_features(ride)
    
    expected_features = {
        "PU_DO": "130_205",
        "trip_distance": 3.66
    }

    assert actual_features == expected_features

class ModelMock():
    def __init__(self, value) -> None:
        self.value = value
        
    def predict(self, X):
        n = len(X)
        return [self.value] * n

# tests should be independent and fast as possible, so no load
# model from S3
def test_predict():
    model= ModelMock(10.0)
    model_service = model_test.ModelService(model)

    features = {
        "PU_DO": "130_205",
        "trip_distance": 3.66
    }
    
    prediction = model_service.predict(features)
    expected_prediction = 10.0

    assert prediction == expected_prediction

def test_lambda_handler():
    model= ModelMock(10.0)
    model_version = "test_123"
    model_service = model_test.ModelService(model, model_version)

    base64_input = read_text("data_test.b64")
    event = {
        "Records": [{
            "kinesis": {
                "data": base64_input,
            },
        }]
    }
    actual_predictions = model_service.lambda_handler(event)
    expected_prediction_event = {
            'predictions': [{
                'model': 'ride_duration_prediction_model',
                'version': model_version,
                'prediction': {
                    'ride_duration': 10.0,
                    'ride_id': 256
                }
            }]
    }

    assert actual_predictions == expected_prediction_event
