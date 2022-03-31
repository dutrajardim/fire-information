"""
# Copy Station Files

This DAG is responsible for ...
"""

from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.data_quality import DataQualityOperator
from operators.spark_on_k8s_app import SparkOnK8sAppOperator
from operators.load_to_s3 import LoadToS3Operator

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
    "copy_ghcn_files_dag",
    default_args=default_args,
    description="Copy ghcn files from ncdc to S3",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
)

dag.doc_md = __doc__

# creating a symbolic task to show the DAG begin
start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

load_data = LoadToS3Operator(
    task_id="Load_ghcn_data_from_ncdc_to_s3",
    s3fs_conn_id="local_minio_conn_id",
    url="ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{{ execution_date.strftime('%Y') }}.csv.gz",
    pathname="dutrajardim-fi/src/ncdc/ghcn/{{ execution_date.strftime('%Y') }}.csv.gz",
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
        ("S3_STATIONS_PATH", "s3a://dutrajardim-fi/tables/stations.parquet"),
        ("S3_GHCN_PATH", "s3a://dutrajardim-fi/tables/ghcn.parquet"),
        ("S3_GHCN_SRC_PATH", "s3a://dutrajardim-fi/src/ncdc/ghcn/2022.csv.gz"),
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
                FROM ghcn
            """,
            "expected_result": 1,
            "error_message": "The number of stored data is not greater than 0!",
        }
    ],
    register_s3_tables=[
        ("ghcn", "dutrajardim-fi/tables/ghcn.parquet/*/*/*/year=2022/*")
    ],
    dag=dag,
)

# creating a symbolic task to show the DAG end
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> load_data
load_data >> submit_spark_app
submit_spark_app >> run_quality_checks
run_quality_checks >> end_operator
