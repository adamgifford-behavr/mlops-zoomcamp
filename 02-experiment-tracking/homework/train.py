import argparse
import os
import pickle

import mlflow
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error


def load_pickle(filename: str):
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)


def run(data_path):
    mlflow.set_experiment("mlops-zoom-hw2")
    mlflow.sklearn.autolog()

    X_train, y_train = load_pickle(os.path.join(data_path, "train.pkl"))
    X_valid, y_valid = load_pickle(os.path.join(data_path, "valid.pkl"))

    with mlflow.start_run():
        # mlflow.log_param("train-data-path", os.path.join(data_path, "train.pkl"))
        # mlflow.log_param("valid-data-path", os.path.join(data_path, "valid.pkl"))

        rf = RandomForestRegressor(max_depth=10, random_state=0)
        rf.fit(X_train, y_train)
        y_pred = rf.predict(X_valid)

        rmse = mean_squared_error(y_valid, y_pred, squared=False)
        mlflow.log_metric("rmse", rmse)

    mlflow.sklearn.autolog(disable=True)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_path",
        default="./output",
        help="the location where the processed NYC taxi trip data was saved.",
    )
    args = parser.parse_args()

    run(args.data_path)
