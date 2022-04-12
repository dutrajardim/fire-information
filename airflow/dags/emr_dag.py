"""
# Complete ETL using Amazon EMR and S3

This DAG is responsible for copying remote data (shapes, firms and ghcn)
to S3 and then creating parquet tables to make ghcn and firms data analysis
from the perspective of administrative areas (cities, states) more easily.
"""

# fmt: off
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from operators.load_to_s3 import LoadToS3Operator
from operators.firms import FirmsOperator
from helpers.emr_templates import EmrTemplates
from airflow.utils.trigger_rule import TriggerRule
from operators.data_quality import DataQualityOperator
from airflow.hooks.base import BaseHook

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from datetime import (datetime, timedelta)
import os
# fmt: on

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
with DAG(
    "emr_dag",
    default_args=default_args,
    description="Dag to test emr operators",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
    params={
        "s3fs_conn_id": "aws_s3_conn_id",
        "s3_bucket": "dutrajardim-fi",
        "skip_load_stations_data": False,
        "skip_load_shapes_data": False,
    },
) as dag:
    dag.doc_md = __doc__

    # creating a symbolic task to show the DAG begin
    start_operator = DummyOperator(task_id="begin_execution")

    # PART 1
    # Station Data
    # Creating tasks to load data s3
    # (making it optional in dag run config).
    with TaskGroup(group_id="load_stations_data") as load_stations_data:
        skip = DummyOperator(task_id="skip")
        data_from_ncdc_to_s3 = LoadToS3Operator(
            task_id="data_from_ncdc_to_s3",
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            url="ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt",
            pathname="{{ params.s3_bucket }}/src/ncdc/stations.txt.gz",
            gz_compress=True,
        )
        check = BranchPythonOperator(
            task_id="check",
            python_callable=lambda dag_run, **kwargs: (
                "load_stations_data.skip"
                if (
                    "skip_load_stations_data" in dag_run.conf
                    and dag_run.conf["skip_load_stations_data"]
                )
                else "load_stations_data.data_from_ncdc_to_s3"
            ),
        )
        join = DummyOperator(
            task_id="join",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        # defining tasks relations in the cur group
        check >> [data_from_ncdc_to_s3, skip]
        [data_from_ncdc_to_s3, skip] >> join

    # Shapes Data
    # Creating tasks to load data to s3
    # (making it optional in dag run config).
    with TaskGroup(group_id="load_shapes_data") as load_shapes_data:

        # configuring osm-boundaries api url
        api_key = "0c8ebd61aff7ea97178ec2ee8936aea1"  # osm key
        srid = "4326"
        file_format = "EWKT"
        db = "osm20220207"
        min_admin_level = 2
        max_admin_level = 8

        # defining countries shapes to be extract
        countries = {"BRA": "-59470", "ARG": "-286393"}

        # making it optional. it involves some work,
        # but helps for debugging
        skip = DummyOperator(task_id="skip")
        check = BranchPythonOperator(
            task_id="check",
            python_callable=lambda dag_run, **kwargs: (
                "load_shapes_data.skip"
                if (
                    "skip_load_shapes_data" in dag_run.conf
                    and dag_run.conf["skip_load_shapes_data"]
                )
                else "load_shapes_data.start"
            ),
        )
        join = DummyOperator(
            task_id="join",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        # start and complete tasks encapsulate load tasks
        start = DummyOperator(task_id="start")
        complete = DummyOperator(task_id="complete")

        # create parallel tasks for each country selected
        for name_iso, country_osm_id in countries.items():
            url = f"https://osm-boundaries.com/Download/Submit?apiKey={api_key}&db={db}&osmIds={country_osm_id}&recursive&minAdminLevel={min_admin_level}&maxAdminLevel={max_admin_level}&format={file_format}&srid={srid}"

            load_task = LoadToS3Operator(
                task_id=f"{name_iso}_{min_admin_level}_{max_admin_level}",
                s3fs_conn_id="{{ params.s3fs_conn_id }}",
                url=url,
                pathname=f"{'{{ params.s3_bucket }}'}/src/shapes/osm/adm_{min_admin_level}_{max_admin_level}/{name_iso}.{file_format.lower()}.gz",
            )

            start >> load_task
            load_task >> complete

        # defining tasks relations in the cur group
        check >> [start, skip]
        [complete, skip] >> join

    # Latest Firms Data
    # Creating tasks to load data to s3
    with TaskGroup(group_id="load_latest_firms_data") as load_latest_firms_data:

        # getting api credential
        conn = BaseHook.get_connection("firms_token")
        token = conn.password
        details_url = conn.host

        # Nasa keeps the last two months of daily
        # text files available for download via HTTPS.
        #
        # According NASA README:
        # The Active Fire Text files are posted at approximately 00:00 UTC
        # each morning. The file continues to be updated 3 hours after each satellite over pass
        # (so the text file changes throughout the day).
        get_firms_details = FirmsOperator(
            task_id="get_firms_details",
            details_url=details_url,
            date="{{ (dag_run.logical_date - macros.timedelta(days=1)) | ds }}",  # yesterday (YYYY-MM-DD)
        )

        # defining the path to save the file
        pathname = "%s/src/firms/suomi_viirs_c2/%s/%s.csv.gz" % (
            "{{ params.s3_bucket }}",
            "{{ (dag_run.logical_date - macros.timedelta(days=1)).strftime('%Y') }}",
            "{{ task_instance.xcom_pull(task_ids='load_latest_firms_data.get_firms_details', key='filename')}}",
        )

        # loading the file to s3
        load_file = LoadToS3Operator(
            task_id="load_file",
            s3fs_conn_id="{{ params.s3_bucket }}",
            url="{{ task_instance.xcom_pull(task_ids='load_latest_firms_data.get_firms_details', key='link') }}",
            gz_compress=True,
            pathname=pathname,
            headers={"Authorization": f"Bearer {token}"},
        )

        get_firms_details >> load_file

    # PART 2
    # Loading local files to s3.
    # These files will be used in the spark/emr context.
    with TaskGroup(group_id="load_script_files") as load_script_files:

        # here, files are supposed to be in a relative path to the DAG script
        cur_dirname = os.path.dirname(os.path.realpath(__file__))

        # symbolic task
        start = DummyOperator(task_id="start")

        # create parallel tasks to upload the files to s3
        for filename in [
            "osm_shapes_spark_etl.py",
            "stations_spark_etl.py",
            "firms_spark_etl.py",
            "bootstrap-actions.sh",
        ]:

            # defining file path
            spark_script_path = os.path.join(cur_dirname, "pyspark_scripts", filename)

            # task to load from remote to s3
            file_to_s3 = LoadToS3Operator(
                task_id=f"{filename}",
                s3fs_conn_id="{{ params.s3fs_conn_id }}",
                url=f"file://{spark_script_path}",  # local file
                pathname=f"{'{{ params.s3_bucket }}'}/spark_scripts/{filename}",
            )

            # defining tasks relations in the cur group
            start >> file_to_s3

    # PART 3
    # Submitting the spark application to Amazon EMR.
    with TaskGroup(group_id="submit_spark_to_emr") as submit_spark_to_emr:

        # helper script to build emr templates
        emr_templates = EmrTemplates()

        # creating a new cluster
        create_cluster = EmrCreateJobFlowOperator(
            task_id="create_cluster",
            job_flow_overrides=emr_templates.get_job_flow_overrides(
                ec2_subnet_id="subnet-0d995a0886cc8d7da",  # it grants EMR access to S3
                key_name="dutrajardim",
                bootstrap_actions=[  # install python apache sedona in the cluster
                    {
                        "Name": "Install dependencies",
                        "ScriptBootstrapAction": {
                            "Path": "s3://{{ params.s3_bucket }}/spark_scripts/bootstrap-actions.sh"
                        },
                    }
                ],
            ),
            retries=0,
            aws_conn_id="aws_default",
            emr_conn_id="emr_default",
        )

        steps = []

        # Extracts hierarchical relations between administrative areas from shapes
        # and save both, relations and shapes, to parquet.
        shapes_spark_step = emr_templates.get_spark_step(
            name="Extracts relations of adm areas from shapes and saves relations and shapes to s3",
            script_path="s3a://{{ params.s3_bucket }}/spark_scripts/osm_shapes_spark_etl.py",
            arguments=[
                (
                    "--s3-osm-shapes-path",
                    "s3a://{{ params.s3_bucket }}/tables/shapes/osm/shapes.parquet",
                ),
                (
                    "--s3-osm-relations-path",
                    "s3a://{{ params.s3_bucket }}/tables/shapes/osm/relations.parquet",
                ),
                (
                    "--s3-osm-shapes-src-path",
                    "s3a://{{ params.s3_bucket }}/src/shapes/osm/*/*",
                ),
            ],
        )

        # This extracts s3 stations data loaded from ncdc to s3,
        # makes a spatial join with geo shapes of administrative areas and
        # then stores the result back to S3.
        stations_spark_step = emr_templates.get_spark_step(
            name="Spatial join of stations with geo shapes of administrative areas",
            script_path="s3a://{{ params.s3_bucket }}/spark_scripts/stations_spark_etl.py",
            arguments=[
                (
                    "--s3-stations-src-path",
                    "s3a://{{ params.s3_bucket }}/src/ncdc/stations.txt.gz",
                ),
                (
                    "--s3-stations-path",
                    "s3a://{{ params.s3_bucket }}/tables/stations/osm_adm8.parquet",
                ),
                (
                    "--s3-shapes-path",
                    "s3a://{{ params.s3_bucket }}/tables/shapes/osm/shapes.parquet/adm=8",
                ),
                (
                    "--s3-relations-path",
                    "s3a://{{ params.s3_bucket }}/tables/shapes/osm/relations.parquet/adm=8",
                ),
            ],
        )

        # This extracts s3 firms data loaded from nasa to s3,
        # makes a spatial join with geo shapes of administrative areas and
        # then stores the result back to S3.
        firms_spark_step = emr_templates.get_spark_step(
            name="Spatial join of FIRMS with geo shapes of administrative areas",
            script_path="s3a://{{ params.s3_bucket }}/spark_scripts/firms_spark_etl.py",
            arguments=[
                (
                    "--s3-firms-src-path",
                    "s3a://%s/src/firms/suomi_viirs_c2/%s"
                    % (
                        "{{ params.s3_bucket }}",
                        "{{ (dag_run.logical_date - macros.timedelta(days=1)).strftime('%Y') }}",
                    ),
                ),
                (
                    "--s3-firms-path",
                    "s3a://{{ params.s3_bucket }}/tables/firms/osm_adm8.parquet",
                ),
                (
                    "--s3-shapes-path",
                    "s3a://{{ params.s3_bucket }}/tables/shapes/osm/shapes.parquet/adm=8",
                ),
                (
                    "--s3-relations-path",
                    "s3a://{{ params.s3_bucket }}/tables/shapes/osm/relations.parquet/adm=8",
                ),
            ],
        )

        steps.append(shapes_spark_step)
        steps.append(stations_spark_step)
        steps.append(firms_spark_step)

        # submitting the script to the cluster
        add_steps = EmrAddStepsOperator(
            task_id="add_steps",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.create_cluster', key='return_value') }}",
            aws_conn_id="aws_default",
            retries=0,
            steps=steps,
        )

        watches = []

        # watch script progress
        watch_oms_shapes_step = EmrStepSensor(
            task_id="watch_oms_shapes_step",
            job_flow_id="{{ task_instance.xcom_pull('submit_spark_to_emr.create_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.add_steps', key='return_value')[0] }}",
            aws_conn_id="aws_default",
            retries=2,
            retry_delay=timedelta(seconds=15),
        )
        watches.append(watch_oms_shapes_step)

        # watch script progress
        watch_stations_step = EmrStepSensor(
            task_id="watch_stations_step",
            job_flow_id="{{ task_instance.xcom_pull('submit_spark_to_emr.create_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.add_steps', key='return_value')[1] }}",
            aws_conn_id="aws_default",
            retries=2,
            retry_delay=timedelta(seconds=15),
        )
        watches.append(watch_stations_step)

        # watch script progress
        watch_firms_step = EmrStepSensor(
            task_id="watch_firms_step",
            job_flow_id="{{ task_instance.xcom_pull('submit_spark_to_emr.create_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.add_steps', key='return_value')[2] }}",
            aws_conn_id="aws_default",
            retries=2,
            retry_delay=timedelta(seconds=15),
        )
        watches.append(watch_firms_step)

        # terminating the cluster (the trigger rule make sure emr
        # will be closed even after a step error)
        terminate_cluster = EmrTerminateJobFlowOperator(
            task_id="terminate_cluster",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.create_cluster', key='return_value') }}",
            retries=5,
            retry_delay=timedelta(seconds=90),
            trigger_rule=TriggerRule.ALL_DONE,
        )

        # defining tasks relations in the cur group
        create_cluster >> add_steps
        add_steps >> watches
        watches >> terminate_cluster

    # PART 4
    # creating the quality tests
    with TaskGroup(group_id="run_quality_tests") as run_quality_tests:
        DataQualityOperator(
            task_id="check_if_data_exists",
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            sql="""
                SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                FROM firms
                WHERE datetime >= STRFTIME('{{(dag_run.logical_date - macros.timedelta(days=2)).strftime('%Y-%m-%d')}}', '%Y-%m-%d')
            """,
            expected_result=1,
            error_message="The number of stored firms is not greater than 0!",
            register_s3_tables=[
                (
                    "firms",
                    "%s/tables/firms/osm_adm8.parquet/year=%s/month=%s/*"
                    % (
                        "{{ params.s3_bucket }}",
                        "{{ (dag_run.logical_date - macros.timedelta(days=2)).strftime('%Y') }}",
                        "{{ (dag_run.logical_date - macros.timedelta(days=2)).month }}",
                    ),
                )
            ],
        )

    # creating a symbolic task to show the DAG end
    end_operator = DummyOperator(task_id="Stop_execution")

    # defining tasks relations
    start_operator >> [
        load_script_files,
        load_stations_data,
        load_shapes_data,
        load_latest_firms_data,
    ]

    [
        load_script_files,
        load_stations_data,
        load_shapes_data,
        load_latest_firms_data,
    ] >> submit_spark_to_emr

    submit_spark_to_emr >> run_quality_tests
    run_quality_tests >> end_operator
