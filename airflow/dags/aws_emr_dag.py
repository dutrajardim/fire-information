"""
# Complete ETL using Amazon EMR and S3

This DAG is responsible for copying remote data (shapes, firms and ghcn)
to S3 and then creating parquet tables to make ghcn and firms data available for
analysis from the perspective of administrative areas (cities, states).
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
from airflow.models import Variable

from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

from datetime import (datetime, timedelta)
import os
# fmt: on

# defining default arguments
default_args = {
    "owner": "Rafael Dutra Jardim",
    "start_date": datetime.utcnow().replace(hour=7, minute=0, second=0, microsecond=0),
    "end_date": None,
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}

# creating the DAG
with DAG(
    "aws_emr_dag",
    default_args=default_args,
    description="Complete ETL for GHCN and FIRMS data (do not load old data)",
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=True,
    params={
        "s3fs_conn_id": "aws_s3_conn_id",
        "s3_bucket": Variable.get("S3_FI_BUCKET", default_var="dutrajardim-fi"),
        "skip_load_stations_data": False,
        "skip_load_shapes_data": False,
        "skip_load_ghcn_data": False,
    },
) as dag:
    dag.doc_md = __doc__

    # creating a symbolic task to show the DAG begin
    start_operator = DummyOperator(task_id="begin_execution")

    # PART 1 (Load remote data)

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

        # Nasa keeps the last two months of daily
        # text files available for download via HTTPS.
        #
        # According NASA README:
        # The Active Fire Text files are posted at approximately 00:00 UTC
        # each morning. The file continues to be updated 3 hours after each satellite over pass
        # (so the text file changes throughout the day).
        get_firms_details = FirmsOperator(
            task_id="get_firms_details",
            firms_conn_id="firms_token",
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
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            url="{{ task_instance.xcom_pull(task_ids='load_latest_firms_data.get_firms_details', key='link') }}",
            gz_compress=True,
            pathname=pathname,
            headers={
                "Authorization": "Bearer {{ task_instance.xcom_pull(task_ids='load_latest_firms_data.get_firms_details', key='token') }}"
            },
        )

        get_firms_details >> load_file

    # Last year of GHCN
    # Creating tasks to load data to s3
    with TaskGroup(group_id="load_ghcn_data") as load_ghcn_data:
        skip = DummyOperator(task_id="skip")
        data_from_ghcn_to_s3 = LoadToS3Operator(
            task_id="data_from_ghcn_to_s3",
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            url="ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{{ dag_run.logical_date.strftime('%Y') }}.csv.gz",
            pathname="{{ params.s3_bucket }}/src/ncdc/ghcn/{{ dag_run.logical_date.strftime('%Y') }}.csv.gz",
        )
        check = BranchPythonOperator(
            task_id="check",
            python_callable=lambda dag_run, **kwargs: (
                "load_ghcn_data.skip"
                if (
                    "skip_load_ghcn_data" in dag_run.conf
                    and dag_run.conf["skip_load_ghcn_data"]
                )
                else "load_ghcn_data.data_from_ghcn_to_s3"
            ),
        )
        join = DummyOperator(
            task_id="join",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        # defining tasks relations in the cur group
        check >> [data_from_ghcn_to_s3, skip]
        [data_from_ghcn_to_s3, skip] >> join

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
            "ghcn_spark_etl.py",
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

        # defining spark steps
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

        # This extracts s3 ghc data loaded from NOAA to s3,
        # makes a spatial join with stations (administrative areas) and
        # then stores the result back to S3.
        ghcn_spark_step = emr_templates.get_spark_step(
            name="Spatial join of GHCN with stations (with administrative areas)",
            script_path="s3a://{{ params.s3_bucket }}/spark_scripts/ghcn_spark_etl.py",
            arguments=[
                (
                    "--s3-stations-path",
                    "s3a://{{ params.s3_bucket }}/tables/stations/osm_adm8.parquet",
                ),
                (
                    "--s3-ghcn-path",
                    "s3a://{{ params.s3_bucket }}/tables/ghcn/osm_adm8.parquet",
                ),
                (
                    "--s3-ghcn-src-path",
                    "s3a://{{ params.s3_bucket }}/src/ncdc/ghcn/{{ dag_run.logical_date.strftime('%Y') }}.csv.gz",
                ),
            ],
        )

        # the steps order is used
        # for setting the watches
        steps.append(shapes_spark_step)  # first
        steps.append(stations_spark_step)  # second
        steps.append(firms_spark_step)  # third
        steps.append(ghcn_spark_step)  # fourth

        # submitting the script to the cluster
        add_steps = EmrAddStepsOperator(
            task_id="add_steps",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.create_cluster', key='return_value') }}",
            aws_conn_id="aws_default",
            retries=0,
            steps=steps,
        )

        # creating watch tasks for the steps
        watches = []

        # watch for shapes
        watch_oms_shapes_step = EmrStepSensor(
            task_id="watch_oms_shapes_step",
            job_flow_id="{{ task_instance.xcom_pull('submit_spark_to_emr.create_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.add_steps', key='return_value')[0] }}",
            aws_conn_id="aws_default",
            retries=2,
            retry_delay=timedelta(seconds=15),
        )
        watches.append(watch_oms_shapes_step)

        # watcher for stations
        watch_stations_step = EmrStepSensor(
            task_id="watch_stations_step",
            job_flow_id="{{ task_instance.xcom_pull('submit_spark_to_emr.create_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.add_steps', key='return_value')[1] }}",
            aws_conn_id="aws_default",
            retries=2,
            retry_delay=timedelta(seconds=15),
        )
        watches.append(watch_stations_step)

        # watcher for firms
        watch_firms_step = EmrStepSensor(
            task_id="watch_firms_step",
            job_flow_id="{{ task_instance.xcom_pull('submit_spark_to_emr.create_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.add_steps', key='return_value')[2] }}",
            aws_conn_id="aws_default",
            retries=2,
            retry_delay=timedelta(seconds=15),
        )
        watches.append(watch_firms_step)

        # watcher for ghcn
        watch_ghcn_step = EmrStepSensor(
            task_id="watch_ghcn_step",
            job_flow_id="{{ task_instance.xcom_pull('submit_spark_to_emr.create_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.add_steps', key='return_value')[3] }}",
            aws_conn_id="aws_default",
            retries=2,
            retry_delay=timedelta(seconds=15),
        )
        watches.append(watch_ghcn_step)

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

        # checking recent firms data
        DataQualityOperator(
            task_id="check_recent_firms_data",
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            sql="""
                SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                FROM firms
                WHERE CAST(STRFTIME(datetime,'%Y-%m-%d') AS DATE) >= '{{(dag_run.logical_date - macros.timedelta(days=3)).strftime('%Y-%m-%d')}}'
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
                    ["datetime"],
                )
            ],
        )

        # checking recent ghcn data
        DataQualityOperator(
            task_id="check_recent_ghcn_data",
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            sql="""
                SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                FROM ghcn
                WHERE CAST(STRFTIME(datetime,'%Y-%m-%d') AS DATE) >= '{{(dag_run.logical_date - macros.timedelta(days=7)).strftime('%Y-%m-%d')}}'
            """,
            expected_result=1,
            error_message="The number of stored data is not greater than 0!",
            register_s3_tables=[
                (
                    "ghcn",
                    "%s/tables/ghcn/osm_adm8.parquet/*/year=%s/*/*"
                    % (
                        "{{ params.s3_bucket }}",
                        "{{ dag_run.logical_date.strftime('%Y') }}",
                    ),
                    ["datetime"],
                )
            ],
        )

    # creating a symbolic task to show the DAG end
    end_operator = DummyOperator(task_id="Stop_execution")

    # defining tasks relations
    load_data_tasks = [
        load_script_files,
        load_stations_data,
        load_shapes_data,
        load_latest_firms_data,
        load_ghcn_data,
    ]

    start_operator >> load_data_tasks
    load_data_tasks >> submit_spark_to_emr
    submit_spark_to_emr >> run_quality_tests
    run_quality_tests >> end_operator
