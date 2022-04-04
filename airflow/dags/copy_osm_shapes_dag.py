"""
# Copy Shape Files

This DAG is responsible for ...
"""

from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from operators.data_quality import DataQualityOperator
from operators.spark_on_k8s_app import SparkOnK8sAppOperator
from operators.load_to_s3 import LoadToS3Operator

import os
import requests

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
    "copy_osm_shape_files_dag",
    default_args=default_args,
    description="Copy shape files from GADM to S3",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
)

dag.doc_md = __doc__

# creating a symbolic task to show the DAG begin
start_operator = DummyOperator(task_id="Begin_execution", dag=dag)


with TaskGroup(group_id="Load_osm_files_to_s3", dag=dag) as load_to_s3_group:

    api_key = "0c8ebd61aff7ea97178ec2ee8936aea1"
    srid = "4326"
    file_format = "EWKT"
    db = "osm20220207"
    min_admin_level = 2
    max_admin_level = 8

    countries = {"BRA": "-59470", "ARG": "-286393"}

    # brazil id = , argentina id = -286393
    for name_iso, country_osm_id in countries.items():
        url = f"https://osm-boundaries.com/Download/Submit?apiKey={api_key}&db={db}&osmIds={country_osm_id}&recursive&minAdminLevel={min_admin_level}&maxAdminLevel={max_admin_level}&format={file_format}&srid={srid}"

        LoadToS3Operator(
            task_id=f"Load_shapes_from_osm_{name_iso}_{min_admin_level}_{max_admin_level}_to_s3",
            s3fs_conn_id="local_minio_conn_id",
            url=url,
            pathname=f"dutrajardim-fi/src/shapes/osm/adm_{min_admin_level}_{max_admin_level}/{name_iso}.{file_format.lower()}.gz",
            dag=dag,
        )


submit_spark_app = SparkOnK8sAppOperator(
    task_id="Submit_osm_shapes_application",
    name="shapes-spark-script",
    main_application_file="s3a://dutrajardim-fi/spark_scripts/shapes_spark_etl.py",
    k8s_conn_id="local_k8s_conn_id",
    spark_app_name="DJ - Shapes Information",
    s3fs_conn_id="local_minio_conn_id",
    envs=[
        ("S3_OSM_SHAPES_PATH", "s3a://dutrajardim-fi/tables/shapes/osm/shapes.parquet"),
        (
            "S3_OSM_SHAPES_RELATIONS_PATH",
            "s3a://dutrajardim-fi/tables/shapes/osm/relations.parquet",
        ),
        ("S3_OSM_SHAPES_SRC_PATH", "s3a://dutrajardim-fi/src/shapes/osm/*/*"),
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
                FROM shapes
            """,
            "expected_result": 1,
            "error_message": "The number of stored shapes is not greater than 0!",
        }
    ],
    register_s3_tables=[
        ("shapes", "dutrajardim-fi/tables/shapes/osm/shapes.parquet/adm=2")
    ],
    dag=dag,
)

# creating a symbolic task to show the DAG end
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> load_to_s3_group
load_to_s3_group >> submit_spark_app
submit_spark_app >> run_quality_checks
run_quality_checks >> end_operator
