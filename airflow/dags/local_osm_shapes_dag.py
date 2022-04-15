"""
# Shape Files ETL

This DAG is responsible for copying remote shapes from Open Street Map
to S3. It also extract hierarchical relation between administrative levels
and store it as a table to make it available for others ELT (Firms and Stations).
"""

# fmt: off
from airflow.utils.task_group import TaskGroup
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from operators.data_quality import DataQualityOperator
from operators.spark_on_k8s_app import SparkOnK8sAppOperator
from operators.load_to_s3 import LoadToS3Operator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import (datetime, timedelta)
import os
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
    "local_osm_shape_dag",
    default_args=default_args,
    description="Copy shape files from GADM to S3",
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    params={
        "s3fs_conn_id": "local_minio_conn_id",
        "skip_load_shapes_data": False,
        "s3_bucket": "dutrajardim-fi",
    },
) as dag:

    dag.doc_md = __doc__

    # creating a symbolic task to show the DAG begin
    start_operator = DummyOperator(task_id="begin_execution")

    # PART 1
    # Creating tasks to load data to s3
    # (making it optional in dag run config).
    with TaskGroup(group_id="load_shapes_data") as load_shapes_data:

        # configuring osm-boundaries api url
        api_key = "0c8ebd61aff7ea97178ec2ee8936aea1"  # osm key
        srid = "4326"
        file_format = "EWKT"
        db = "osm20220207"
        min_admin_level = 2
        max_admin_level = 8

        # defining countries shapes to be extract
        countries = {"BRA": "-59470", "ARG": "-286393"}

        # making it optional. it involves some work,
        # but helps for debugging
        skip = DummyOperator(task_id="skip")
        check = BranchPythonOperator(
            task_id="check",
            python_callable=lambda dag_run, **kwargs: (
                "load_shapes_data.skip"
                if (
                    "skip_load_shapes_data" in dag_run.conf
                    and dag_run.conf["skip_load_shapes_data"]
                )
                else "load_shapes_data.start"
            ),
        )
        join = DummyOperator(
            task_id="join",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        # start and complete tasks encapsulate load tasks
        start = DummyOperator(task_id="start")
        complete = DummyOperator(task_id="complete")

        # create parallel tasks for each country selected
        for name_iso, country_osm_id in countries.items():
            url = f"https://osm-boundaries.com/Download/Submit?apiKey={api_key}&db={db}&osmIds={country_osm_id}&recursive&minAdminLevel={min_admin_level}&maxAdminLevel={max_admin_level}&format={file_format}&srid={srid}"

            load_task = LoadToS3Operator(
                task_id=f"{name_iso}_{min_admin_level}_{max_admin_level}",
                s3fs_conn_id="{{ params.s3fs_conn_id }}",
                url=url,
                pathname=f"{'{{ params.s3_bucket }}'}/src/shapes/osm/adm_{min_admin_level}_{max_admin_level}/{name_iso}.{file_format.lower()}.gz",
            )

            start >> load_task
            load_task >> complete

        # defining tasks relations in the cur group
        check >> [start, skip]
        [complete, skip] >> join

    # PART 2
    # Loading spark script file to s3.
    # This file is used in the spark context.
    cur_dirname = os.path.dirname(os.path.realpath(__file__))
    spark_script_path = os.path.join(
        cur_dirname, "pyspark_scripts", "osm_shapes_spark_etl.py"
    )

    script_to_s3 = LoadToS3Operator(
        task_id="load_firms_spark_script_to_s3",
        s3fs_conn_id="{{ params.s3fs_conn_id }}",
        url=f"file://{spark_script_path}",  # local file
        pathname="{{ params.s3_bucket }}/spark_scripts/osm_shapes_spark_etl.py",
        dag=dag,
    )

    # PART 3
    # Submitting the spark application to join.
    # This extracts s3 stations data loaded from ncdc to s3,
    # makes a spatial join with geo shapes of administrative and
    # the result table is saved back to S3.
    submit_spark_app = SparkOnK8sAppOperator(
        task_id="Submit_osm_shapes_application",
        name="shapes-spark-script",
        main_application_file="s3a://{{ params.s3_bucket }}/spark_scripts/osm_shapes_spark_etl.py",
        k8s_conn_id="local_k8s_conn_id",
        spark_app_name="DJ - Shapes Information",
        s3fs_conn_id="{{ params.s3fs_conn_id }}",
        arguments=[
            (
                "--s3-osm-shapes-path",
                "s3a://{{ params.s3_bucket }}/tables/shapes/osm/shapes.parquet",
            ),
            (
                "--s3-osm-relations-path",
                "s3a://{{ params.s3_bucket }}/tables/shapes/osm/relations.parquet",
            ),
            (
                "--s3-osm-shapes-src-path",
                "s3a://{{ params.s3_bucket }}/src/shapes/osm/*/*",
            ),
        ],
    )

    # PART 4
    # creating the quality tests
    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        s3fs_conn_id="{{ params.s3fs_conn_id }}",
        sql="""
            SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
            FROM shapes
        """,
        expected_result=1,
        error_message="The number of stored shapes is not greater than 0!",
        register_s3_tables=[
            ("shapes", "{{ params.s3_bucket }}/tables/shapes/osm/shapes.parquet/adm=2")
        ],
    )

    # creating a symbolic task to show the DAG end
    end_operator = DummyOperator(task_id="stop_execution")

    # defining tasks relations
    start_operator >> [load_shapes_data, script_to_s3]
    [load_shapes_data, script_to_s3] >> submit_spark_app
    submit_spark_app >> run_quality_checks
    run_quality_checks >> end_operator
