"""
# Copy Shape Files

This DAG is responsible for ...
"""

from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from operators.data_quality import DataQualityOperator
from operators.shapefile_to_parquet import ShapefileToParquetOperator
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
    "copy_shape_files_dag",
    default_args=default_args,
    description="Copy shape files from GADM to S3",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
)

dag.doc_md = __doc__

# creating a symbolic task to show the DAG begin
start_operator = DummyOperator(task_id="Begin_execution", dag=dag)


def pathname_callable(filename, **kwargs):
    basename = os.path.basename(filename)
    name, _ = os.path.splitext(basename)
    src, iso_country, adm_level = name.split("-")

    return "dutrajardim-fi/src/shapes/geo_boundaries/adm_%s/%s/%s" % (
        adm_level[-1],
        iso_country,
        basename,
    )


def unzip_filter(filename):
    basename = os.path.basename(filename)
    _, extension = os.path.splitext(basename)

    return extension in [".shp", ".shx", ".dbf"] and "simplified" not in basename


with TaskGroup(group_id="Load_gadm_files_to_s3", dag=dag) as load_to_s3_group:

    for country in ["BRA", "URY"]:
        r = requests.get(
            "https://www.geoboundaries.org/api/current/gbOpen/%s/ALL" % country
        )

        for adm in r.json():

            LoadToS3Operator(
                task_id="Load_shapes_from_gb_%s__to_s3" % adm["boundaryID"],
                s3fs_conn_id="local_minio_conn_id",
                url=adm["downloadURL"],
                pathname_callable=pathname_callable,
                unzip=True,
                unzip_filter=unzip_filter,
                dag=dag,
            )


common_config = {
    "select_expr": (
        "geometry",
        "shapeID AS id",
        "shapeName AS name",
        "shapeGroup as group",
    ),
}

shapefile_to_parquet_adm0 = ShapefileToParquetOperator(
    task_id="Shapefile_to_parquet_adm0",
    s3fs_conn_id="local_minio_conn_id",
    path_shp="s3://dutrajardim-fi/src/shapes/geo_boundaries/adm_0/*/*",
    path_pq="s3://dutrajardim-fi/tables/shapes/geo_boundaries/adm0.parquet",
    **common_config,
    dag=dag,
)

shapefile_to_parquet_adm1 = ShapefileToParquetOperator(
    task_id="Shapefile_to_parquet_adm1",
    s3fs_conn_id="local_minio_conn_id",
    path_shp="s3://dutrajardim-fi/src/shapes/geo_boundaries/adm_1/*/*",
    path_pq="s3://dutrajardim-fi/tables/shapes/geo_boundaries/adm1.parquet",
    **common_config,
    partition_cols=["group"],
    dag=dag,
)

shapefile_to_parquet_adm2 = ShapefileToParquetOperator(
    task_id="Shapefile_to_parquet_adm2",
    s3fs_conn_id="local_minio_conn_id",
    path_shp="s3://dutrajardim-fi/src/shapes/geo_boundaries/adm_2/*/*",
    path_pq="s3://dutrajardim-fi/tables/shapes/geo_boundaries/adm2.parquet",
    **common_config,
    partition_cols=["group"],
    dag=dag,
)

shapefile_to_parquet_adm3 = ShapefileToParquetOperator(
    task_id="Shapefile_to_parquet_adm3",
    s3fs_conn_id="local_minio_conn_id",
    path_shp="s3://dutrajardim-fi/src/shapes/geo_boundaries/adm_3/*/*",
    path_pq="s3://dutrajardim-fi/tables/shapes/geo_boundaries/adm3.parquet",
    **common_config,
    partition_cols=["group"],
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
                FROM adm2
            """,
            "expected_result": 1,
            "error_message": "The number of stored shapes is not greater than 0!",
        }
    ],
    register_s3_tables=[
        ("adm2", "dutrajardim-fi/tables/shapes/geo_boundaries/adm2.parquet")
    ],
    dag=dag,
)

# creating a symbolic task to show the DAG end
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> load_to_s3_group

load_to_s3_group >> shapefile_to_parquet_adm0
load_to_s3_group >> shapefile_to_parquet_adm1
load_to_s3_group >> shapefile_to_parquet_adm2
load_to_s3_group >> shapefile_to_parquet_adm3

shapefile_to_parquet_adm0 >> run_quality_checks
shapefile_to_parquet_adm1 >> run_quality_checks
shapefile_to_parquet_adm2 >> run_quality_checks
shapefile_to_parquet_adm3 >> run_quality_checks

run_quality_checks >> end_operator
