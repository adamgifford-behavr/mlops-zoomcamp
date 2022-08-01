#!/usr/bin/env python
# coding: utf-8

from datetime import datetime

import batch
import pandas as pd
from deepdiff import DeepDiff


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)

def test_prepare_data():
    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),        
    ]

    columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
    df = pd.DataFrame(data, columns=columns)

    categorical = ['PUlocationID', 'DOlocationID']
    actual_df = batch.prepare_data(df, categorical)
    
    expected_df = pd.DataFrame(data={
          "PUlocationID": ["-1", "1"], 
          "DOlocationID": ["-1", "1"],
          "pickup_datetime": [dt(1, 2), dt(1, 2)],    
          "dropOff_datetime": [dt(1, 10), dt(1, 10)],
          "duration": [8., 8.],
    })

    diff = DeepDiff(actual_df.to_dict(), expected_df.to_dict(), significant_digits=1)
    # assert actual_response == predicted_response
    print(f"diff={diff}")

    assert "values_changed" not in diff
    assert "type_changes" not in diff
