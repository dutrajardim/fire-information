#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cat >> ~/.bashrc <<EOF
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
export AIRFLOW__CORE__DAGS_FOLDER="$SCRIPT_DIR/airflow/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$SCRIPT_DIR/airflow/plugins"
EOF