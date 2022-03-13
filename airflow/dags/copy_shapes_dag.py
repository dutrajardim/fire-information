"""
# Copy Shape Files

This DAG is responsible for ...
"""

from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.data_quality import DataQualityOperator

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


def load_data_callable():
    m_client = get_minio_client()

    # countries
    countries_str = "BRA, ARG, PRY, URY"
    countries = countries_str.replace(" ", "").split(",")

    url_str = "https://geodata.ucdavis.edu/gadm/gadm4.0/shp/gadm40_{}_shp.zip"
    urls = map(lambda x: url_str.format(x), countries)

    s3_bucket = "dutrajardim-fi"

    for url in urls:
        with closing(urllib.request.urlopen(url)) as resource:
            stream = io.BytesIO(resource.read())

            with closing(zipfile.ZipFile(stream, "r")) as zip_archive:
                for filename in zip_archive.namelist():

                    basename = os.path.basename(filename)
                    name, _ = os.path.splitext(basename)
                    version, iso_country, adm_level = name.split("_")

                    file_stream = io.BytesIO(zip_archive.open(filename).read())
                    file_size = file_stream.getbuffer().nbytes

                    s3_object_key = "src/shapes/%s/adm_%s/%s/%s".format(
                        version, adm_level, iso_country, basename
                    )

                    m_client.put_object(
                        s3_bucket, s3_object_key, file_stream, file_size
                    )


# load shapes from GADM site to s3
load_data = PythonOperator(
    task_id="Load_data_from_GADM_to_s3", python_callable=load_data_callable, dag=dag
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

start_operator >> run_quality_checks
run_quality_checks >> end_operator
