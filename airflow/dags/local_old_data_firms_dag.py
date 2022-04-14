"""
# Firms Files ETL

This DAG is responsible for loading remote old FIRMS data
from nasa to s3 and then create parquet table defining in which
administrative area the fire spot occurred.
"""

# fmt: off
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from operators.firms import FirmsOperator
from operators.load_to_s3 import LoadToS3Operator
from airflow.hooks.base import BaseHook
from operators.spark_on_k8s_app import SparkOnK8sAppOperator
from operators.data_quality import DataQualityOperator

from datetime import (datetime, timedelta)
import os
# fmt: on

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
    "local_old_data_firms_dag",
    default_args=default_args,
    description=(
        "Load remote old FIRMS data from nasa "
        "to s3 and then create parquet table defining in which"
        "administrative area the fire spot occurred."
    ),
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
    params={
        "s3fs_conn_id": "local_minio_conn_id",
        "s3_bucket": "dutrajardim-fi",
    },
) as dag:

    # years of data to extract
    years = ["2021"]

    dag.__dict__

    # creating a symbolic task to show the DAG begin
    start_operator = DummyOperator(task_id="begin_execution")

    # PART 1
    # Creating tasks to load data to s3
    with TaskGroup(group_id="load_old_firms_data") as load_latest_firms_data:

        for year in years:

            # function to create the path name
            # where data will be stored in s3
            # based in zip content file name
            pathname_callable = (
                lambda filename, params, year=year, **kwargs: "%s/src/firms/suomi_viirs_c2/archive/%s/%s.gz"
                % (params["s3_bucket"], year, os.path.basename(filename))
            )

            # loading the file to s3
            LoadToS3Operator(
                task_id=f"load_file_{year}",
                s3fs_conn_id="{{ params.s3fs_conn_id }}",
                url=f"https://firms.modaps.eosdis.nasa.gov/data/country/zips/viirs-snpp_{year}_all_countries.zip",
                pathname_callable=pathname_callable,
                unzip=True,
                gz_compress=True,
            )

    # PART 2
    # Loading spark script file to s3.
    # This file is used in the spark context.
    cur_dirname = os.path.dirname(os.path.realpath(__file__))
    spark_script_path = os.path.join(
        cur_dirname, "pyspark_scripts", "firms_spark_etl.py"
    )
    script_to_s3 = LoadToS3Operator(
        task_id="load_firms_spark_script_to_s3",
        s3fs_conn_id="{{ params.s3fs_conn_id }}",
        url=f"file://{spark_script_path}",  # local file
        pathname="{{ params.s3_bucket }}/spark_scripts/firms_spark_etl.py",
    )

    # PART 3
    # Submitting the spark application to join.
    # This add the administrative area information for the fire information
    # and save to a parquet table
    submit_spark_app = SparkOnK8sAppOperator(
        task_id="submit_spark_application_join_firms_adm",
        name="firms-spark-script",
        main_application_file="s3a://{{ params.s3_bucket }}/spark_scripts/firms_spark_etl.py",
        k8s_conn_id="local_k8s_conn_id",
        spark_app_name="DJ - Firms Information",
        s3fs_conn_id="{{ params.s3fs_conn_id }}",
        arguments=[
            (
                "--s3-firms-src-path",
                "s3a://%s/src/firms/suomi_viirs_c2/archive/{%s}"
                % ("{{ params.s3_bucket }}", ",".join(years)),
            ),
            (
                "--s3-firms-path",
                "s3a://{{ params.s3_bucket }}/tables/firms/osm_adm8.parquet",
            ),
            (
                "--s3-shapes-path",
                "s3a://{{ params.s3_bucket }}/tables/shapes/osm/shapes.parquet/adm=8",
            ),
            (
                "--s3-relations-path",
                "s3a://{{ params.s3_bucket }}/tables/shapes/osm/relations.parquet/adm=8",
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
                    FROM firms) a
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
                    "firms",
                    "{{ params.s3_bucket }}/tables/firms/osm_adm8.parquet",
                    ["year"],
                )
            ],
        )

    # creating a symbolic task to show the DAG end
    end_operator = DummyOperator(task_id="end_execution")

    # defining tasks relations
    start_operator >> [load_latest_firms_data, script_to_s3]
    [load_latest_firms_data, script_to_s3] >> submit_spark_app
    submit_spark_app >> run_quality_tests
    run_quality_tests >> end_operator
