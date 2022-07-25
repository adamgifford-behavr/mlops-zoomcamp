# pylint: disable=line-too-long
import json

import requests
from deepdiff import DeepDiff

with open("event_test.json", "rt", encoding="utf-8") as f_in:
    event = json.load(f_in)


url = 'http://localhost:8080/2015-03-31/functions/function/invocations'
actual_response = requests.post(url, json=event).json()
print(json.dumps(actual_response, indent=2))

predicted_response = {
    'predictions': [
        {
            'model': 'ride_duration_prediction_model',
            'version': "72f8ea3dcf6548789a08b95f3a6d3375",
            'prediction': {'ride_duration': 18.2, 'ride_id': 256},
        }
    ]
}

diff = DeepDiff(actual_response, predicted_response, significant_digits=1)
# assert actual_response == predicted_response
print(f"diff={diff}")

assert "values_changed" not in diff
assert "type_changes" not in diff
