#!/usr/bin/env python
# coding: utf-8

import pickle
import pandas as pd
import sys
import os

def generate_ride_ids(index, year, month):
    return f'{year:04d}/{month:02d}_' + index.astype('str')

def read_data(filename, year, month):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df["ride_id"] = generate_ride_ids(df.index, year, month)

    return df

def prepare_dictionaries(df):
    categorical = ['PUlocationID', 'DOlocationID']
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    dicts = df[categorical].to_dict(orient='records')
    return dicts

def load_model(filename):
    with open(filename, 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    return dv, lr

def apply_model(input_file, year, month, output_file):
    print(f"reading the data from {input_file}...")
    df = read_data(input_file, year, month)
    dicts = prepare_dictionaries(df)

    print(f"loading the model from local filestore...")
    dv, lr = load_model("model.bin")

    print("applying the model...")    
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print(f"mean predicted duration is {y_pred.mean():.2f}")

    df_result = pd.DataFrame()
    df_result["ride_id"] = df["ride_id"]
    df_result["predicted_duration"] = y_pred

    print(f"saving the results to {output_file}...")
    df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )


def run():
    taxi_type = sys.argv[1]
    year = int(sys.argv[2])
    month = int(sys.argv[3])

    # the :0Xd tells f-string to make the number have X digits
    input_file = f'https://nyc-tlc.s3.amazonaws.com/trip+data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'output/{taxi_type}/{year:04d}-{month:02d}.parquet'

    if not os.path.exists(f"output/{taxi_type}"):
        os.mkdir(f"output/{taxi_type}")

    apply_model(input_file=input_file, year=year, month=month, output_file=output_file)


if __name__ == "__main__":
    run()




