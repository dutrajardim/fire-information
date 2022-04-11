from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from operators.load_to_s3 import LoadToS3Operator

from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from pyarrow import csv
import os

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
    "test_dag",
    default_args=default_args,
    description="Copy firms files fro",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
) as dag:

    dag.__dict__

    start_operator = DummyOperator(task_id="begin_execution")

    with TaskGroup(group_id="load_latest_firms_data") as load_latest_firms_data:
        data_url = "https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/details/FIRMS/suomi-npp-viirs-c2/Global?fields=all&formats=csv"
        data = csv.read_csv(urlopen(data_url))
        token = "cmFmYWVsZGphcmRpbTpjbUZtWVdWc1pHcGhjbVJwYlVCbmJXRnBiQzVqYjIwPToxNjQ5NjMwMDU4OjgyNTkyMTI0YzMwZGQxYzM1YzY3YjBhNWY1NGIxNWViMGY4MDk1NjE"

        for link in data["downloadsLink"][-6:]:
            basename = os.path.basename(f"{link}")
            filename, _ = os.path.splitext(basename)

            LoadToS3Operator(
                task_id=f"{filename}",
                s3fs_conn_id="local_minio_conn_id",
                url=link,
                gz_compress=True,
                pathname=f"dutrajardim-fi/src/firms/suomi_viirs_c2/{{{{ execution_date.strftime('%Y') }}}}/{filename}.csv.gz",
                headers={"Authorization": f"Bearer {token}"},
            )

    # value = DagRun.execution_date()
    # value = dag.execution_date()
    # middle = DummyOperator(task_id=f"{value}")

    # data_url = "https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/details/FIRMS/suomi-npp-viirs-c2/Global?fields=all&formats=csv"
    # df_data = pd.read_csv(data_url)

    # df[df["last_modified"] > "2022-04-09"]['downloadsLink'].values

    # url = 'https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/suomi-npp-viirs-c2/Global/SUOMI_VIIRS_C2_Global_VNP14IMGTDL_NRT_2022100.txt'
    # req = Request(url, headers={'Authorization': f"Bearer {token}"})

    # with urlopen(req) as resource:
    #     resp = csv.read_csv(resource)

    # from urllib.request import Request, urlopen

    end_operator = DummyOperator(task_id="end_execution")

    start_operator >> load_latest_firms_data
    load_latest_firms_data >> end_operator
