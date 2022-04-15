# !/bin/bash


# Install Airflow using the constraints file
AIRFLOW_VERSION=2.2.5
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.6
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.2.5/constraints-3.6.txt
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" 

# project dependencies
pip install s3fs pyarrow==7.0.0 duckdb==0.3.2 kubernetes apache-airflow-providers-amazon pache-airflow-providers-cncf-kubernetes


