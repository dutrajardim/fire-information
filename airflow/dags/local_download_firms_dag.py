"""
# Firms Files ETL

This DAG is responsible for loading remote FIRMS data
from nasa to s3.
"""

# fmt: off
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from operators.firms import FirmsOperator
from operators.load_to_s3 import LoadToS3Operator

from datetime import (datetime, timedelta)
# fmt: on

default_args = {
    "owner": "Rafael Dutra Jardim",
    # "start_date": datetime.utcnow(),
    "start_date": datetime(2022, 4, 2, 7, 0, 0),
    "end_date": datetime.utcnow(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}

# creating the DAG
with DAG(
    "local_download_firms_dag",
    default_args=default_args,
    description="Load remote FIRMS data from nasa to s3.",
    schedule_interval="@daily",
    max_active_runs=3,
    catchup=True,
    params={"s3fs_conn_id": "local_minio_conn_id", "s3_bucket": "dutrajardim-fi"},
) as dag:

    dag.__dict__

    # creating a symbolic task to show the DAG begin
    start_operator = DummyOperator(task_id="begin_execution")

    # Creating tasks to load data to s3
    with TaskGroup(group_id="load_firms_data") as load_firms_data:

        # Nasa keeps the last two months of daily
        # text files available for download via HTTPS.
        #
        # According NASA README:
        # The Active Fire Text files are posted at approximately 00:00 UTC
        # each morning. The file continues to be updated 3 hours after each satellite over pass
        # (so the text file changes throughout the day).
        get_firms_details = FirmsOperator(
            task_id="get_firms_details",
            firms_conn_id="firms_token",
            date="{{ (dag_run.logical_date - macros.timedelta(days=1)) | ds }}",  # yesterday (YYYY-MM-DD)
        )

        # defining the path to save the file
        pathname = "%s/src/firms/suomi_viirs_c2/%s/%s.csv.gz" % (
            "{{ params.s3_bucket }}",
            "{{ (dag_run.logical_date - macros.timedelta(days=1)).strftime('%Y') }}",
            "{{ task_instance.xcom_pull(task_ids='load_firms_data.get_firms_details', key='filename')}}",
        )

        # loading the file to s3
        load_file = LoadToS3Operator(
            task_id="load_file",
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            url="{{ task_instance.xcom_pull(task_ids='load_firms_data.get_firms_details', key='link') }}",
            gz_compress=True,
            pathname=pathname,
            headers={
                "Authorization": "Bearer {{ task_instance.xcom_pull(task_ids='load_latest_firms_data.get_firms_details', key='token') }}"
            },
        )

        get_firms_details >> load_file

    # creating a symbolic task to show the DAG end
    end_operator = DummyOperator(task_id="end_execution")

    # defining tasks relations
    start_operator >> load_firms_data
    load_firms_data >> end_operator
