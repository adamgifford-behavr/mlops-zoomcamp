#!/usr/bin/env bash

# when see first error, exit script with non-zero error code
# set -e
# but, this stops other things we might want to run after error found like
# docker-compse down

cd "$(dirname "$0")"

LOCAL_TAG=`date +"%Y-%m-%d-%H-%M"`
LOCAL_IMAGE_NAME="stream-model-duration:${LOCAL_TAG}"
export RUN_ID=$RUN_ID
export EXP_ID=$EXP_ID
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION
export PREDICTIONS_STREAM_NAME="ride_predictions"

docker build -t ${LOCAL_IMAGE_NAME} -f ../Dockerfile_test ..

docker-compose up -d

sleep 3

aws --endpoint-url=http://localhost:4566 \
    kinesis create-stream \
    --stream-name ${PREDICTIONS_STREAM_NAME} \
    --shard-count 1

sleep 5

pipenv run python test_docker_test.py

# we can write any error codes from testing to a variable
ERROR_CODE=$?

# print out logs if error
if [ ${ERROR_CODE} != 0 ]; then
    docker-compose logs
    docker-compose down
    exit ${ERROR_CODE}
fi

pipenv run python test_kinesis_test.py

# we can write any error codes from testing to a variable
ERROR_CODE=$?

# print out logs if error
if [ ${ERROR_CODE} != 0 ]; then
    docker-compose logs
    docker-compose down
    exit ${ERROR_CODE}
fi

docker-compose down