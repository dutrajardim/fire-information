"""
# Copy Station Files

This DAG is responsible for extracting old ghcn data, loading from ncdc to s3,
making a join with stations (with administrative areas) and storing
the result back to s3.
"""

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from operators.data_quality import DataQualityOperator
from operators.spark_on_k8s_app import SparkOnK8sAppOperator
from operators.load_to_s3 import LoadToS3Operator

import os
from datetime import datetime, timedelta

# defining default arguments
default_args = {
    "owner": "Rafael Dutra Jardim",
    "start_date": datetime.utcnow(),
    # 'start_date': datetime(2018, 11, 1, 0, 0, 0),
    # 'end_date': datetime(2018, 11, 1, 0, 50, 0),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}

# creating the DAG
with DAG(
    "local_old_data_ghcn_dag",
    default_args=default_args,
    description=(
        "Extracts old ghcn data and makes a join"
        "with it and stations data (with administrative areas)."
    ),
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
    params={"s3fs_conn_id": "local_minio_conn_id", "s3_bucket": "dutrajardim-fi"},
) as dag:

    # years of data to extract
    years = ["2021"]

    dag.doc_md = __doc__

    # creating a symbolic task to show the DAG begin
    start_operator = DummyOperator(task_id="begin_execution")

    # PART 1
    # Creating tasks to load data to s3
    # (making it optional in dag run config).
    with TaskGroup(group_id="load_ghcn_data") as load_ghcn_data:
        for year in years:
            load_year = LoadToS3Operator(
                task_id=f"data_from_ghcn_to_s3_{year}",
                s3fs_conn_id="{{ params.s3fs_conn_id }}",
                url=f"ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz",
                pathname=f"dutrajardim-fi/src/ncdc/ghcn/{year}.csv.gz",
            )

    # PART 2
    # Loading spark script file to s3.
    # This file is used in the spark context.
    cur_dirname = os.path.dirname(os.path.realpath(__file__))
    spark_script_path = os.path.join(
        cur_dirname, "pyspark_scripts", "ghcn_spark_etl.py"
    )

    script_to_s3 = LoadToS3Operator(
        task_id="load_ghcn_spark_script_to_s3",
        s3fs_conn_id="{{ params.s3fs_conn_id }}",
        url=f"file://{spark_script_path}",  # local file
        pathname="{{ params.s3_bucket }}/spark_scripts/ghcn_spark_etl.py",
        dag=dag,
    )

    # PART 3
    # Submitting the spark application to join.
    # This extracts s3 ghcn data loaded from ncdc to s3 and
    # makes a join with stations (with administrative areas).
    submit_spark_app = SparkOnK8sAppOperator(
        task_id="submit_spark_application_join_ghcn_adm",
        name="ghcn-spark-script",
        main_application_file="s3a://{{ params.s3_bucket }}/spark_scripts/ghcn_spark_etl.py",
        k8s_conn_id="local_k8s_conn_id",
        spark_app_name="DJ - GHCN Information",
        s3fs_conn_id="{{ params.s3fs_conn_id }}",
        arguments=[
            (
                "--s3-stations-path",
                "s3a://{{ params.s3_bucket }}/tables/stations/osm_adm8.parquet",
            ),
            (
                "--s3-ghcn-path",
                "s3a://{{ params.s3_bucket }}/tables/ghcn/osm_adm8.parquet",
            ),
            (
                "--s3-ghcn-src-path",
                "s3a://%s/src/ncdc/ghcn/{%s}.csv.gz"
                % ("{{ params.s3_bucket }}", ",".join(years)),
            ),
        ],
    )

    # PART 4
    # creating the quality tests
    with TaskGroup(group_id="run_quality_tests") as run_quality_tests:

        # creating the quality tests
        # checking if there isn't data for the years selected
        DataQualityOperator(
            task_id="check_if_data_exists",
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            sql="""
                SELECT COUNT(*)
                FROM
                    (SELECT DISTINCT(year)
                    FROM ghcn) a
                RIGHT JOIN
                    (SELECT UNNEST([%s]) as year) b
                ON a.year = b.year
                WHERE a.year IS NULL
            """
            % ",".join(years),
            expected_result=0,
            error_message="The number of stored data for each year is not greater than 0!",
            register_s3_tables=[
                (
                    "ghcn",
                    "{{ params.s3_bucket }}/tables/ghcn/osm_adm8.parquet",
                    ["year"],
                )
            ],
        )

# creating a symbolic task to show the DAG end
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

# defining tasks relations
start_operator >> [load_ghcn_data, script_to_s3]
[load_ghcn_data, script_to_s3] >> submit_spark_app
submit_spark_app >> run_quality_tests
run_quality_tests >> end_operator
