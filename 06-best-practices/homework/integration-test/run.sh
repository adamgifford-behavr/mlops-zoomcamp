#!/usr/bin/env bash

cd "$(dirname "$0")"

export INPUT_FILE_PATTERN=$INPUT_FILE_PATTERN
export OUTPUT_FILE_PATTERN=$OUTPUT_FILE_PATTERN
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION

if [ "${LOCAL_IMAGE_NAME}" == "" ]; then 
    LOCAL_TAG=`date +"%Y-%m-%d-%H-%M"`
    export LOCAL_IMAGE_NAME="batch-model-duration:${LOCAL_TAG}"
    echo "LOCAL_IMAGE_NAME is not set, building a new image with tag ${LOCAL_IMAGE_NAME}"
    docker build -t ${LOCAL_IMAGE_NAME} ..
else
    echo "no need to build image ${LOCAL_IMAGE_NAME}"
fi

docker-compose up -d

sleep 5

aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration

sleep 5

pipenv run python integration_test.py

# we can write any error codes from testing to a variable
ERROR_CODE=$?

# print out logs if error
if [ ${ERROR_CODE} != 0 ]; then
    docker-compose logs
    docker-compose down
    exit ${ERROR_CODE}
fi

docker-compose down