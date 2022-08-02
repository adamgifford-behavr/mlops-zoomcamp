# test:
# 	echo test

# # this format means run depends on test
# run: test
# 	echo 123

# this command causes date to be run every time LOCAL_TAG is needed
# thus, datetime may be different across separate uses of LOCAL_TAG or LOCAL_IMAGE_NAME
# as different steps are run sequentially and take time. hence, we use `shell` to fix
# the datetime to only when LOCAL_TAG is defined
# LOCAL_TAG:=`date + "%Y-%m-%d-%H-%M"`
LOCAL_TAG:=$(shell date +"%Y-%m-%d-%H-%M")
LOCAL_IMAGE_NAME:=stream-model-duration:${LOCAL_TAG}

test:
	pytest tests/

quality_checks:
	isort .
	black .
	pylint --recursive=y .

build: quality_checks test
	docker build -t ${LOCAL_IMAGE_NAME} .

integration_test: build
#   LOCAL_IMAGE_NAME=${LOCAL_IMAGE_NAME} bash integration-test/run.sh
	LOCAL_IMAGE_NAME=${LOCAL_IMAGE_NAME} bash integration-test/run_test.sh

publish: build integration_test
	LOCAL_IMAGE_NAME=${LOCAL_IMAGE_NAME} bash scripts/publish.sh

setup:
	pipenv install --dev
	pre-commit install
