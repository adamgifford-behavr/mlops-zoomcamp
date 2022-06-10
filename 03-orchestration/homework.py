import os
import pickle
from datetime import datetime

import mlflow
import pandas as pd
from dateutil.relativedelta import relativedelta
from dotenv import find_dotenv, load_dotenv
from prefect import flow, task
from prefect.deployments import DeploymentSpec
from prefect.engine import FlowRunContext, get_run_logger
from prefect.flow_runners import SubprocessFlowRunner
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import SequentialTaskRunner
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error


@task
def read_data(path):
    df = pd.read_parquet(path)
    return df


@task
def prepare_features(df, categorical, train=True):
    parent_flow_run_context = FlowRunContext.get()
    parent_logger = get_run_logger(parent_flow_run_context)

    df["duration"] = df.dropOff_datetime - df.pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    mean_duration = df.duration.mean()
    if train:
        parent_logger.info(f"The mean duration of training is {mean_duration}")
    else:
        parent_logger.info(f"The mean duration of validation is {mean_duration}")

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")
    return df


@task
def train_model(df, categorical):
    parent_flow_run_context = FlowRunContext.get()
    parent_logger = get_run_logger(parent_flow_run_context)

    train_dicts = df[categorical].to_dict(orient="records")
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts)
    y_train = df.duration.values

    parent_logger.info(f"The shape of X_train is {X_train.shape}")
    parent_logger.info(f"The DictVectorizer has {len(dv.feature_names_)} features")

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        mlflow.set_tag("model", "LinearRegression")
        lr = LinearRegression()
        mlflow.log_params(lr.get_params())
        lr.fit(X_train, y_train)
        y_pred = lr.predict(X_train)
        rmse = mean_squared_error(y_train, y_pred, squared=False)
        mlflow.log_metric("train_rmse", rmse)

    parent_logger.info(f"The RMSE of training is: {rmse}")
    return lr, dv, run_id


@task
def run_model(df, categorical, dv, lr, run_id):
    parent_flow_run_context = FlowRunContext.get()
    parent_logger = get_run_logger(parent_flow_run_context)

    val_dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(val_dicts)
    y_pred = lr.predict(X_val)
    y_val = df.duration.values

    with mlflow.start_run(run_id=run_id) as run:
        rmse = mean_squared_error(y_val, y_pred, squared=False)
        mlflow.log_metric("val_rmse", rmse)

    parent_logger.info(f"The RMSE of validation is: {rmse}")
    return


@task
def get_paths(date):
    FILE_PATH = "./data/"
    FILE_PREF = "fhv_tripdata_"
    FILE_SUF = ".parquet"

    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    train_date = date + relativedelta(months=-2)
    train_yr = str(train_date.year)
    if train_date.month < 10:
        train_mo = "0" + str(train_date.month)
    else:
        train_mo = str(train_date.month)

    val_date = date + relativedelta(months=-1)
    val_yr = str(val_date.year)
    if train_date.month < 10:
        val_mo = "0" + str(val_date.month)
    else:
        val_mo = str(val_date.month)

    train_path = FILE_PATH + FILE_PREF + train_yr + "-" + train_mo + FILE_SUF
    val_path = FILE_PATH + FILE_PREF + val_yr + "-" + val_mo + FILE_SUF
    return train_path, val_path


@flow(task_runner=SequentialTaskRunner())
def main(date=None):
    if date is None:
        date = datetime.strftime(datetime.today().date(), "%Y-%m-%d")

    load_dotenv(find_dotenv())

    TRACKING_SERVER_HOST = os.environ.get("TRACKING_SERVER_HOST")
    mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:5000")
    mlflow.set_experiment("nyc-fhv-taxi-experiment")

    parent_flow_run_context = FlowRunContext.get()
    parent_logger = get_run_logger(parent_flow_run_context)

    train_path, val_path = get_paths(date).result()

    categorical = ["PUlocationID", "DOlocationID"]

    df_train = read_data(train_path)
    df_train_processed = prepare_features(df_train, categorical)

    df_val = read_data(val_path)
    df_val_processed = prepare_features(df_val, categorical, False)

    # train the model
    lr, dv, run_id = train_model(df_train_processed, categorical).result()
    run_model(df_val_processed, categorical, dv, lr, run_id)

    preproc_filepath = "preprocessors/dv-" + date + ".b"
    with open(preproc_filepath, "wb") as f_out:
        pickle.dump(dv, f_out)

    model_filepath = "models/model-" + date + ".bin"
    with open(model_filepath, "wb") as f_out:
        pickle.dump(lr, f_out)

    with mlflow.start_run(run_id=run_id) as run:
        mlflow.set_tag("developer", "adam")
        mlflow.log_param("train-data-path", train_path)
        mlflow.log_param("valid-data-path", val_path)
        mlflow.log_artifact(preproc_filepath, artifact_path="preprocessors")
        mlflow.log_artifact(model_filepath, artifact_path="models")


# if __name__ == '__main__':
#     import sys
#     if len(sys.argv)==2:
#         date = sys.argv[1]
#     else:
#         date = None

#     main(date=date)

DeploymentSpec(
    name="cron-schedule-deployment",
    flow=main,
    schedule=CronSchedule(cron="0 9 15 * *", timezone="America/New_York"),
    tags=["mlopszoom-hw3"],
    flow_runner=SubprocessFlowRunner(),
)
