"""
# Copy Station Files

This DAG is responsible for ...
"""

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from airflow.operators.dummy_operator import DummyOperator
from operators.data_quality import DataQualityOperator
from operators.spark_on_k8s_app import SparkOnK8sAppOperator
from operators.load_to_s3 import LoadToS3Operator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

import os
from datetime import datetime, timedelta
import pandas as pd

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
dag = DAG(
    "copy_firms_files_dag",
    default_args=default_args,
    description="Copy firms files from ncdc to S3",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
)

dag.doc_md = __doc__

# creating a symbolic task to show the DAG begin
start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

years = ["2019", "2020", "2021"]

with TaskGroup(group_id="load_firms_data_from_nasa_to_s3", dag=dag) as load_to_s3_group:
    for year in years:
        pathname_callable = (
            lambda filename, year=year, **kwargs: "dutrajardim-fi/src/firms/suomi_viirs_c2/archive/%s/%s.gz"
            % (year, os.path.basename(filename))
        )

        task = LoadToS3Operator(
            task_id=f"load_firms_data_from_nasa_to_s3-{year}",
            s3fs_conn_id="local_minio_conn_id",
            url=f"https://firms.modaps.eosdis.nasa.gov/data/country/zips/viirs-snpp_{year}_all_countries.zip",
            pathname_callable=pathname_callable,
            unzip=True,
            gz_compress=True,
            dag=dag,
        )

skip_operator = DummyOperator(task_id="skip_load_data", dag=dag)

branching = BranchPythonOperator(
    task_id="checking_for_skip_load_data",
    python_callable=lambda: "skip_load_data"
    if True
    else "load_firms_data_from_nasa_to_s3",
    dag=dag,
)

join = DummyOperator(
    task_id="join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag
)

cur_dirname = os.path.dirname(os.path.realpath(__file__))
spark_script_path = os.path.join(cur_dirname, "pyspark_scripts", "firms_spark_etl.py")

script_to_s3 = LoadToS3Operator(
    task_id="load_firms_spark_script_to_s3",
    s3fs_conn_id="local_minio_conn_id",
    url=f"file://{spark_script_path}",  # local file
    pathname="dutrajardim-fi/spark_scripts/firms_spark_etl.py",
    dag=dag,
)

submit_spark_app = SparkOnK8sAppOperator(
    task_id="Submit_firms_to_parquet_spark_application",
    name="firms-spark-script",
    main_application_file="s3a://dutrajardim-fi/spark_scripts/firms_spark_etl.py",
    k8s_conn_id="local_k8s_conn_id",
    spark_app_name="DJ - FIRMS Information",
    s3fs_conn_id="local_minio_conn_id",
    envs=[
        ("S3_FIRMS_PATH", "s3a://dutrajardim-fi/tables/firms/osm_adm8.parquet"),
        (
            "S3_FIRMS_SRC_PATH",
            "s3a://dutrajardim-fi/src/firms/suomi_viirs_c2/archive/{%s}"
            % ",".join(years),
        ),
        (
            "S3_SHAPES_PATH",
            "s3a://dutrajardim-fi/tables/shapes/osm/shapes.parquet/adm=8",
        ),
        (
            "S3_RELATIONS_PATH",
            "s3a://dutrajardim-fi/tables/shapes/osm/relations.parquet/adm=8",
        ),
    ],
    dag=dag,
)

# creating the quality tests
with TaskGroup(group_id="run_quality_tests", dag=dag) as run_quality_tests:
    for year in years:
        DataQualityOperator(
            task_id=f"check_if_data_exists_{year}",
            s3fs_conn_id="local_minio_conn_id",
            sql="""
                SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                FROM firms
            """,
            expected_result=1,
            error_message="The number of stored data is not greater than 0!",
            register_s3_tables=[
                (
                    "firms",
                    f"s3a://dutrajardim-fi/tables/firms/osm_adm8.parquet/year={year}/*/*",
                )
            ],
            dag=dag,
        )

# creating a symbolic task to show the DAG end
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> [branching, script_to_s3]
branching >> [skip_operator, load_to_s3_group]
[skip_operator, load_to_s3_group] >> join
[join, script_to_s3] >> submit_spark_app
submit_spark_app >> run_quality_tests
run_quality_tests >> end_operator
