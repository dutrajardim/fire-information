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
from helpers import LoadDataCallables, K8sTemplates

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
    "copy_station_files_dag",
    default_args=default_args,
    description="Copy station files from ncdc to S3",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
)

dag.doc_md = __doc__

callables = LoadDataCallables(s3fs_conn_id="local_minio_conn_id")
k8s_templates = K8sTemplates(s3fs_conn_id="local_minio_conn_id")

# creating a symbolic task to show the DAG begin
start_operator = DummyOperator(task_id="Begin_execution", dag=dag)


load_data = PythonOperator(
    task_id="Load_station_data_from_ncdc_to_s3",
    python_callable=callables.ncdc_stations,
    dag=dag,
)

submit_spark_app = SparkOnK8sAppOperator(
    task_id="Submit_stations_to_parquet_spark_application",
    template=k8s_templates.get_stations_application_template(),
    k8s_conn_id="local_k8s_conn_id",
    dag=dag,
)

# creating the quality tests
run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    s3fs_conn_id="local_minio_conn_id",
    dq_checks=[
        {
            "check_sql": """
                SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                FROM {}
            """,
            "s3_table": "dutrajardim-fi/tables/stations.parquet",
            "expected_result": 1,
            "error_message": "The number of stored stations is not greater than 0!",
        }
    ],
    dag=dag,
)

# creating a symbolic task to show the DAG end
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> load_data
load_data >> submit_spark_app
submit_spark_app >> run_quality_checks
run_quality_checks >> end_operator