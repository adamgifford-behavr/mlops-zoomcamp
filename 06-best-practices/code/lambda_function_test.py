import os

import model_test

PREDICTIONS_STREAM_NAME = os.getenv('PREDICTIONS_STREAM_NAME', 'ride_predictions')
RUN_ID = os.getenv('RUN_ID', "72f8ea3dcf6548789a08b95f3a6d3375")
EXP_ID = os.getenv('EXP_ID', "3")
TEST_RUN = os.getenv("TEST_RUN", "False") == "True"

model_service = model_test.init(
    prediction_stream_name=PREDICTIONS_STREAM_NAME,
    exp_id=EXP_ID,
    run_id=RUN_ID,
    test_run=TEST_RUN,
)


def lambda_handler(event, context):
    # pylint: disable=unused-argument
    return model_service.lambda_handler(event)
