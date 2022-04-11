"""
# Station Files ETL

This DAG is responsible for copying remote stations data from ncdc
to s3 and then create parquet table joining information about
in which administrative area (contry, city...) the station
is located.
"""

# fmt: off
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from operators.data_quality import DataQualityOperator
from operators.spark_on_k8s_app import SparkOnK8sAppOperator
from operators.load_to_s3 import LoadToS3Operator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator

import os
from datetime import datetime, timedelta
# fmt: on

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
    "local_station_dag",
    default_args=default_args,
    description="Copy station files from ncdc to S3",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
    params={
        "s3fs_conn_id": "local_minio_conn_id",
        "s3_bucket": "dutrajardim-fi",
        "skip_load_station_data": False,
    },
) as dag:

    dag.doc_md = __doc__

    # creating a symbolic task to show the DAG begin
    start_operator = DummyOperator(task_id="begin_execution")

    # PART 1
    # Creating tasks to load data to s3
    # (making it optional in dag run config).
    with TaskGroup(group_id="load_station_data") as load_station_data:
        skip = DummyOperator(task_id="skip")
        data_from_ncdc_to_s3 = LoadToS3Operator(
            task_id="data_from_ncdc_to_s3",
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            url="ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt",
            pathname="{{ params.s3_bucket }}/src/ncdc/stations.txt.gz",
            gz_compress=True,
        )
        check = BranchPythonOperator(
            task_id="check",
            python_callable=lambda dag_run, **kwargs: (
                "load_station_data.skip"
                if (
                    "skip_load_station_data" in dag_run.conf
                    and dag_run.conf["skip_load_station_data"]
                )
                else "load_station_data.data_from_ncdc_to_s3"
            ),
        )
        join = DummyOperator(
            task_id="join",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        # defining tasks relations in the cur group
        check >> [data_from_ncdc_to_s3, skip]
        [data_from_ncdc_to_s3, skip] >> join

    # PART 2
    # Loading spark script file to s3.
    # This file is used in the spark context.
    cur_dirname = os.path.dirname(os.path.realpath(__file__))
    spark_script_path = os.path.join(
        cur_dirname, "pyspark_scripts", "stations_spark_etl.py"
    )
    script_to_s3 = LoadToS3Operator(
        task_id="load_stations_spark_script_to_s3",
        s3fs_conn_id="{{ params.s3fs_conn_id }}",
        url=f"file://{spark_script_path}",  # local file
        pathname="{{ params.s3_bucket }}/spark_scripts/stations_spark_etl.py",
    )

    # PART 3
    # Submitting the spark application to join.
    # This extracts s3 stations data loaded from ncdc to s3,
    # makes a spatial join with geo shapes of administrative and
    # the result table is saved back to S3.
    submit_spark_app = SparkOnK8sAppOperator(
        task_id="submit_spark_application_join_station_adm",
        name="stations-spark-script",
        main_application_file="s3a://{{ params.s3_bucket }}/spark_scripts/stations_spark_etl.py",
        k8s_conn_id="local_k8s_conn_id",
        spark_app_name="DJ - Station Information",
        s3fs_conn_id="{{ params.s3fs_conn_id }}",
        arguments=[
            (
                "--s3-stations-src-path",
                "s3a://{{ params.s3_bucket }}/src/ncdc/stations.txt.gz",
            ),
            (
                "--s3-stations-path",
                "s3a://{{ params.s3_bucket }}/tables/stations/osm_adm8.parquet",
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
        run_quality_checks = DataQualityOperator(
            task_id="check_if_data_exists",
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            sql="""
                SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                FROM stations
            """,
            expected_result=1,
            error_message="The number of stored stations is not greater than 0!",
            register_s3_tables=[
                ("stations", "{{ params.s3_bucket }}/tables/stations/osm_adm8.parquet")
            ],
        )

    # creating a symbolic task to show the DAG end
    end_operator = DummyOperator(task_id="Stop_execution")

    # defining tasks relations
    start_operator >> [load_station_data, script_to_s3]
    [load_station_data, script_to_s3] >> submit_spark_app
    submit_spark_app >> run_quality_checks
    run_quality_checks >> end_operator
