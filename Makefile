.DEFAULT_GOAL:=bootstrap
.PHONY: bootstrap aws_login bootstrap_build_pipe deploy_dev_app cdk_synth_dev_app prepare_local_invoke service_dependencies build unit_tests infra_tests integration_tests e2e_tests sort_imports check_format_code format_code lint pre-commit pr
.ONESHELL:
SHELL := /bin/bash
ROOT_DIR = $(shell pwd)
USER = $(shell whoami)

CDK_SYNTH_QUITE = -q "false"
export AWS_PROFILE := lia-001

bootstrap: pyproject.toml
	python3 -m venv .venv
	@source .venv/bin/activate && \
	pip install --upgrade pip pre-commit poetry && \
	pre-commit install && \
	poetry config --local virtualenvs.in-project true && \
	poetry install && \
	deactivate

venv: .venv/bin/activate
	. .venv/bin/activate

aws_sso_login:
	aws sso login

bootstrap_build_pipe: bootstrap build
	@echo "Bootstrap build pipe"
	@source .venv/bin/activate && \
	cdk deploy DataScraperPipelineStack --require-approval never && \
	deactivate

deploy_dev_app: build
	@echo "Deploying ./dev-app.py to dev account"
	@source .venv/bin/activate && \
	export STAGE_NAME=${USER} && \
	export AWS_PROFILE=lia-001 && \
	cdk deploy --app "python3 dev_app.py" --require-approval=never && \
	deactivate

destroy_dev_app:
	@echo "Destroying ./dev-app.py in dev account"
	@source .venv/bin/activate && \
	export STAGE_NAME=${USER} && \
	export AWS_PROFILE=lia-001 && \
	cdk destroy --app "python3 dev_app.py" --force && \
	deactivate

cdk_synth_dev_app:
	@echo "cdk synthesizing ./dev-app.py"
	@source .venv/bin/activate && \
	export STAGE_NAME=${USER} && \
	export AWS_PROFILE=lia-001 && \
	cdk synth ${CDK_SYNTH_QUITE} --app "python3 dev_app.py" && \
	deactivate

prepare_local_invoke:
	@echo "Prepare ./dev-app.py template for local invoke with sam cli"
	@source .venv/bin/activate && \
	export STAGE_NAME=${USER} && \
	export AWS_PROFILE=lia-001 && \
	cdk synth ${CDK_SYNTH_QUITE} --app "python3 dev_app.py" --no-staging > template.json && \
	deactivate

service_dependencies:
	@source .venv/bin/activate && \
	poetry export --only=dev --without-hashes --format=requirements.txt > dev_requirements.txt && \
	poetry export --only=buildpipe --without-hashes --format=requirements.txt > buildpipe_requirements.txt && \
	poetry export --without=dev,buildpipe --without-hashes --format=requirements.txt > lambda_requirements.txt && \
	deactivate

build: service_dependencies
	@source .venv/bin/activate && \
	mkdir -p .build/lambdas && \
	cp -r device_manager .build/lambdas && \
	mkdir -p .build/common_layer && \
	poetry export --without=dev --without-hashes --format=requirements.txt > .build/common_layer/requirements.txt && \
	deactivate

pipeline_build:
	pip install poetry
	poetry export --only=dev --without-hashes --format=requirements.txt > dev_requirements.txt
	poetry export --without=dev --without-hashes --format=requirements.txt > lambda_requirements.txt
	poetry export --only=buildpipe --without-hashes --format=requirements.txt > buildpipe_requirements.txt
	mkdir -p .build/lambdas
	cp -r device_manager .build/lambdas
	mkdir -p .build/common_layer
	poetry export --without=dev --without-hashes --format=requirements.txt > .build/common_layer/requirements.txt
	pip install -r buildpipe_requirements.txt

unit_tests: build
	@source .venv/bin/activate && \
	pytest tests/unit --cov-config=.coveragerc --cov=device_manager --cov-report xml && \
	deactivate

infra_tests: build
	@source .venv/bin/activate && \
	pytest tests/infrastructure && \
	deactivate

integration_tests: build
	@source .venv/bin/activate && \
	pytest tests/integration --cov-config=.coveragerc --cov=device_manager --cov-report html && \
	deactivate

e2e_tests: venv build
	pytest tests/e2e --cov-config=.coveragerc --cov=device_manager --cov-report xml

sort_imports:
	@source .venv/bin/activate && \
	isort ${ROOT_DIR} && \
	deactivate

check_format_code:
	@source .venv/bin/activate && \
	black ${ROOT_DIR} --check --diff --color && \
	deactivate

format_code:
	@source .venv/bin/activate && \
	black ${ROOT_DIR} && \
	deactivate

lint:
	@source .venv/bin/activate && \
	flake8 device_manager/* infrastructure/* tests/* && \
	deactivate

pre-commit:
	@source .venv/bin/activate && \
	pre-commit run -a --show-diff-on-failure && \
	deactivate

pr: service_dependencies sort_imports format_code lint unit_tests infra_tests deploy_dev_app integration_tests


local_lambda_invoke: CDK_SYNTH_QUITE=-q "true"
local_lambda_invoke: cdk_synth_dev_app
	@sam local invoke $(lambda) --template ./cdk.out/DataScraperStack.template.json --event ./events/sample-lambda/event.json
