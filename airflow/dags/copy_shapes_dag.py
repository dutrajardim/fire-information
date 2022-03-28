"""
# Copy Shape Files

This DAG is responsible for ...
"""

from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.data_quality import DataQualityOperator
from operators.shapefile_to_parquet import ShapefileToParquetOperator
from helpers import LoadDataCallables

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

callables = LoadDataCallables(s3fs_conn_id="local_minio_conn_id")

# creating a symbolic task to show the DAG begin
start_operator = DummyOperator(task_id="Begin_execution", dag=dag)


# load shapes from GADM site to s3
load_gadm_shapes = PythonOperator(
    task_id="Load_shapes_from_GADM_to_s3",
    python_callable=callables.gadm_shapes,
    dag=dag,
)

shapefile_to_parquet_adm0 = ShapefileToParquetOperator(
    task_id="Shapefile_to_parquet_adm0",
    s3fs_conn_id="local_minio_conn_id",
    path_shp="s3://dutrajardim-fi/src/shapes/gadm40/adm_0/*/*",
    path_pq="s3://dutrajardim-fi/tables/shapes/adm0.parquet",
    fields=["COUNTRY", "ID_0"],
    transformations="""
    SELECT geometry, ID_0 AS adm0, COUNTRY AS name
    FROM {table}
    """,
    partition_cols=["adm0"],
    dag=dag,
)

shapefile_to_parquet_adm1 = ShapefileToParquetOperator(
    task_id="Shapefile_to_parquet_adm1",
    s3fs_conn_id="local_minio_conn_id",
    path_shp="s3://dutrajardim-fi/src/shapes/gadm40/adm_1/*/*",
    path_pq="s3://dutrajardim-fi/tables/shapes/adm1.parquet",
    fields=["ID_0", "ID_1", "NAME_1"],
    transformations="""
    SELECT geometry, ID_0 AS adm0, ID_1 AS adm1, NAME_1 AS name
    FROM {table}
    """,
    partition_cols=["adm0"],
    dag=dag,
)

shapefile_to_parquet_adm2 = ShapefileToParquetOperator(
    task_id="Shapefile_to_parquet_adm2",
    s3fs_conn_id="local_minio_conn_id",
    path_shp="s3://dutrajardim-fi/src/shapes/gadm40/adm_2/*/*",
    path_pq="s3://dutrajardim-fi/tables/shapes/adm2.parquet",
    fields=["ID_0", "ID_2", "NAME_2"],
    transformations="""
    SELECT
        geometry, 
        ID_0 AS adm0,
        REGEXP_REPLACE(ID_2, '(.*\..*)\..*', '\\1_1') AS adm1,
        ID_2 AS adm2,
        NAME_2 AS name
    FROM {table}
    """,
    partition_cols=["adm0", "adm1"],
    dag=dag,
)

shapefile_to_parquet_adm3 = ShapefileToParquetOperator(
    task_id="Shapefile_to_parquet_adm3",
    s3fs_conn_id="local_minio_conn_id",
    path_shp="s3://dutrajardim-fi/src/shapes/gadm40/adm_3/*/*",
    path_pq="s3://dutrajardim-fi/tables/shapes/adm3.parquet",
    fields=["ID_0", "ID_3", "NAME_3"],
    transformations="""
    SELECT
        geometry, 
        ID_0 AS adm0,
        REGEXP_REPLACE(ID_3, '(.*\..*)\..*\..*', '\\1_1') AS adm1,
        ID_3 AS adm3,
        NAME_3 AS name
    FROM {table}
    """,
    partition_cols=["adm0", "adm1"],
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
            "s3_table": "dutrajardim-fi/tables/shapes/adm3.parquet",
            "expected_result": 1,
            "error_message": "The number of stored shapes is not greater than 0!",
        }
    ],
    dag=dag,
)

# creating a symbolic task to show the DAG end
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> load_gadm_shapes
load_gadm_shapes >> shapefile_to_parquet_adm0
load_gadm_shapes >> shapefile_to_parquet_adm1
load_gadm_shapes >> shapefile_to_parquet_adm2
load_gadm_shapes >> shapefile_to_parquet_adm3
shapefile_to_parquet_adm0 >> run_quality_checks
shapefile_to_parquet_adm1 >> run_quality_checks
shapefile_to_parquet_adm2 >> run_quality_checks
shapefile_to_parquet_adm3 >> run_quality_checks
run_quality_checks >> end_operator
