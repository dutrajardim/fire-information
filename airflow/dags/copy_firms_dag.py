"""
# Copy Station Files

This DAG is responsible for ...
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup

from airflow.operators.dummy_operator import DummyOperator
from operators.data_quality import DataQualityOperator
from operators.spark_on_k8s_app import SparkOnK8sAppOperator
from operators.load_to_s3 import LoadToS3Operator
import os

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

# url https://firms.modaps.eosdis.nasa.gov/data/download/%file
# files DL_FIRE_SV-C2_251209.zip:2018, DL_FIRE_SV-C2_251208.zip:2019, DL_FIRE_SV-C2_251207.zip:2020, DL_FIRE_SV-C2_249334.zip:2021


with TaskGroup(group_id="Load_firms_data_from_nasa_to_s3", dag=dag) as load_to_s3_group:
    for year in ["2020", "2021"]:
        pathname_callable = (
            lambda filename, year=year, **kwargs: "dutrajardim-fi/src/firms/suomi_viirs_c2/archive/%s/%s.gz"
            % (year, os.path.basename(filename))
        )

        task = LoadToS3Operator(
            task_id="Load_firms_data_from_nasa_to_s3-%s" % year,
            s3fs_conn_id="local_minio_conn_id",
            url="https://firms.modaps.eosdis.nasa.gov/data/country/zips/viirs-snpp_%s_all_countries.zip"
            % year,
            pathname_callable=pathname_callable,
            unzip=True,
            gz_compress=True,
            dag=dag,
        )


submit_spark_app = SparkOnK8sAppOperator(
    task_id="Submit_firms_to_parquet_spark_application",
    name="ghcn-spark-script",
    main_application_file="s3a://dutrajardim-fi/spark_scripts/firms_spark_etl.py",
    k8s_conn_id="local_k8s_conn_id",
    spark_app_name="DJ - FIRMS Information",
    s3fs_conn_id="local_minio_conn_id",
    envs=[
        ("S3_FIRMS_PATH", "s3a://dutrajardim-fi/tables/firms.parquet"),
        (
            "S3_FIRMS_SRC_PATH",
            "s3a://dutrajardim-fi/src/firms/suomi_viirs_c2/archive/{2021,2022}",
        ),
        ("S3_ADM2_SRC_PATH", "s3a://dutrajardim-fi/tables/shapes/adm2.parquet"),
        ("S3_ADM3_SRC_PATH", "s3a://dutrajardim-fi/tables/shapes/adm3.parquet"),
    ],
    dag=dag,
)

# creating the quality tests
run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    s3fs_conn_id="local_minio_conn_id",
    dq_checks=[
        {
            "sql": """
                SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                FROM firms
            """,
            "expected_result": 1,
            "error_message": "The number of stored data is not greater than 0!",
        }
    ],
    register_s3_tables=[
        ("firms", "s3a://dutrajardim-fi/tables/firms.parquet/*/*/year=2021/*")
    ],
    dag=dag,
)

# creating a symbolic task to show the DAG end
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> load_to_s3_group
load_to_s3_group >> submit_spark_app
submit_spark_app >> run_quality_checks
run_quality_checks >> end_operator
