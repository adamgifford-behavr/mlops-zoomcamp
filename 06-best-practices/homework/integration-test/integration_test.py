#!/usr/bin/env python
# coding: utf-8

import os
from datetime import datetime

import pandas as pd
from deepdiff import DeepDiff


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)

# make the test data
data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (1, 1, dt(1, 2, 0), dt(2, 2, 1)),        
]

columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
df = pd.DataFrame(data, columns=columns)

# set the s3 endpoint for saving "raw" data
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}

# save the data
input_file = "s3://nyc-duration/in/2021-01.parquet"
df.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)

# now do the test on fake data via batch.py
os.system("python batch.py 2021 1")

# read the saved data
output_file = "s3://nyc-duration/out/2021-01.parquet"
actual_df = pd.read_parquet(output_file, storage_options=options)
print(actual_df.predicted_duration.sum())

expected_df = pd.DataFrame(data={
        "ride_id": ["2021/01_0", "2021/01_1"],
        "predicted_duration": [23.052085, 46.236612],
})

diff = DeepDiff(actual_df.to_dict(), expected_df.to_dict(), significant_digits=1)
print(f"diff={diff}")

assert "values_changed" not in diff
assert "type_changes" not in diff
