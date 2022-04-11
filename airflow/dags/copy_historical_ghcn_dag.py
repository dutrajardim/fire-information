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

from datetime import datetime, timedelta
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
    "copy_historical_ghcn_files_dag",
    default_args=default_args,
    description="Copy historical ghcn files from ncdc to S3",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
)

dag.doc_md = __doc__

# creating a symbolic task to show the DAG begin
start_operator = DummyOperator(task_id="Begin_execution", dag=dag)


with TaskGroup(group_id="Load_firms_data_from_nasa_to_s3", dag=dag) as load_to_s3_group:
    for year in range(2015, datetime.now().year):

        LoadToS3Operator(
            task_id=f"Load_ghcn_data_{year}_from_ncdc_to_s3",
            s3fs_conn_id="local_minio_conn_id",
            url=f"ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz",
            pathname=f"dutrajardim-fi/src/ncdc/ghcn/{year}.csv.gz",
            dag=dag,
        )

cur_dirname = os.path.dirname(os.path.realpath(__file__))
spark_script_path = os.path.join(cur_dirname, "pyspark_scripts", "ghcn_spark_etl.py")

script_to_s3 = LoadToS3Operator(
    task_id="load_ghcn_spark_script_to_s3",
    s3fs_conn_id="local_minio_conn_id",
    url=f"file://{spark_script_path}",  # local file
    pathname="dutrajardim-fi/spark_scripts/ghcn_spark_etl.py",
    dag=dag,
)

submit_spark_app = SparkOnK8sAppOperator(
    task_id="Submit_ghcn_to_parquet_spark_application",
    name="ghcn-spark-script",
    main_application_file="s3a://dutrajardim-fi/spark_scripts/ghcn_spark_etl.py",
    k8s_conn_id="local_k8s_conn_id",
    spark_app_name="DJ - GHCN Information",
    s3fs_conn_id="local_minio_conn_id",
    envs=[
        ("S3_STATIONS_PATH", "s3a://dutrajardim-fi/tables/stations/osm_amd8.parquet"),
        ("S3_GHCN_PATH", "s3a://dutrajardim-fi/tables/ghcn/osm_amd8.parquet"),
        (
            "S3_GHCN_SRC_PATH",
            "s3a://dutrajardim-fi/src/ncdc/ghcn/*",
        ),
    ],
    dag=dag,
)

# creating the quality tests
run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    s3fs_conn_id="local_minio_conn_id",
    sql="""
        SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
        FROM ghcn
    """,
    expected_result=1,
    error_message="The number of stored data is not greater than 0!",
    register_s3_tables=[
        (
            "ghcn",
            "dutrajardim-fi/tables/ghcn/osm_adm8.parquet/*/*/*/*",
        )
    ],
    dag=dag,
)

# creating a symbolic task to show the DAG end
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> [load_to_s3_group, script_to_s3]
[load_to_s3_group, script_to_s3] >> submit_spark_app
submit_spark_app >> run_quality_checks
run_quality_checks >> end_operator
