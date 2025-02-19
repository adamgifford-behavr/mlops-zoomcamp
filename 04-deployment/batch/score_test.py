#!/usr/bin/env python
# coding: utf-8

import os
import pickle
import sys
import pandas as pd
import mlflow
import uuid

from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import make_pipeline


def generate_uuids(n):
    ride_ids = []
    for i in range(n):
        ride_ids.append(str(uuid.uuid4()))
    return ride_ids

def read_dataframe(filename: str):
    df = pd.read_parquet(filename)

    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)]
    df["ride_id"] = generate_uuids(len(df))

    return df

def prepare_dictionaries(df: pd.DataFrame):
    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)

    df['PU_DO'] = df['PULocationID'] + '_' + df['DOLocationID']
    categorical = ['PU_DO']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')
    return dicts


def load_model(exp_id, run_id):
    logged_model = f"s3://agifford-mlflow-artifacts-remote/{exp_id}/{run_id}/artifacts/model"
    model = mlflow.pyfunc.load_model(logged_model)
    return model

def apply_model(input_file, exp_id, run_id, output_file):
    print(f"reading the data from {input_file}...")
    df = read_dataframe(input_file)
    dicts = prepare_dictionaries(df)

    print(f"loading the model from experiment {exp_id} with RUN_ID={run_id}...")
    model = load_model(exp_id, run_id)

    print("applying the model...")    
    y_pred = model.predict(dicts)

    df_result = pd.DataFrame()
    df_result["ride_id"] = df["ride_id"]
    df_result["lpep_pickup_datetime"] = df["lpep_pickup_datetime"]
    df_result["PULocationID"] = df["PULocationID"]
    df_result["DOLocationID"] = df["DOLocationID"]
    df_result["actual_duration"] = df["duration"]
    df_result["predicted_duration"] = y_pred
    df_result["diff"] = df_result["actual_duration"] - df_result["predicted_duration"]
    df["model_version"] = run_id
    print(f"saving the results to {output_file}...")
    df_result.to_parquet(output_file, index=False)


def run():
    taxi_type = sys.argv[1]
    year = int(sys.argv[2])
    month = int(sys.argv[3])
    exp_id = int(sys.argv[4])
    run_id = sys.argv[5]

    # the :0Xd tells f-string to make the number have X digits
    input_file = f'https://s3.amazonaws.com/nyc-tlc/trip+data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f"output/{taxi_type}/tripdata_{year:04d}-{month:02d}.parquet"

    # EXP_ID = 3
    # RUN_ID = os.getenv("RUN_ID", "72f8ea3dcf6548789a08b95f3a6d3375")

    apply_model(input_file=input_file, exp_id=exp_id, run_id=run_id, output_file=output_file)


if __name__ == "__main__":
    run()


