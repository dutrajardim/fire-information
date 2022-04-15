"""
# Copy Station Files

This DAG is responsible for extracting old ghcn data, loading from ncdc to s3,
making a join with stations (with administrative areas) and storing
the result back to s3.
"""

# fmt: off
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from operators.load_to_s3 import LoadToS3Operator
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
    "aws_old_data_ghcn_dag",
    default_args=default_args,
    description=(
        "Extracts old ghcn data and makes a join"
        "with it and stations data (with administrative areas)."
    ),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    params={
        "s3fs_conn_id": "aws_s3_conn_id",
        "s3_bucket": Variable.get("S3_FI_BUCKET", default_var="dutrajardim-fi"),
        "ec2_subnet_id": Variable.get(
            "EC2_SUBNET_ID", default_var="subnet-0d995a0886cc8d7da"
        ),
        "ec2_key_name": Variable.get("EC2_KEY_NAME", default_var="dutrajardim"),
    },
) as dag:

    # years of data to extract
    years = ["2021"]

    dag.doc_md = __doc__

    # creating a symbolic task to show the DAG begin
    start_operator = DummyOperator(task_id="begin_execution")

    # PART 1
    # Creating tasks to load data to s3
    # (making it optional in dag run config).
    with TaskGroup(group_id="load_ghcn_data") as load_ghcn_data:
        for year in years:
            load_year = LoadToS3Operator(
                task_id=f"data_from_ghcn_to_s3_{year}",
                s3fs_conn_id="{{ params.s3fs_conn_id }}",
                url=f"ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz",
                pathname=f"{'{{ params.s3_bucket }}'}/src/ncdc/ghcn/{year}.csv.gz",
            )

    # PART 2
    # Loading local files to s3.
    # These files will be used in the spark/emr context.
    with TaskGroup(group_id="load_script_files") as load_script_files:

        # here, files are supposed to be in a relative path to the DAG script
        cur_dirname = os.path.dirname(os.path.realpath(__file__))

        # symbolic task
        start = DummyOperator(task_id="start")

        # create parallel tasks to upload the files to s3
        for filename in ["ghcn_spark_etl.py", "bootstrap-actions.sh"]:

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
                ec2_subnet_id="{{ params.ec2_subnet_id }}",  # it grants EMR access to S3
                key_name="{{ params.ec2_key_name }}",
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

        # This extracts s3 ghcn data loaded from ncdc to s3 and
        # makes a join with stations (with administrative areas).
        ghcn_spark_step = emr_templates.get_spark_step(
            name="Join of GHCN with stations (administrative areas)",
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
                    "s3a://%s/src/ncdc/ghcn/{%s}.csv.gz"
                    % ("{{ params.s3_bucket }}", ",".join(years)),
                ),
            ],
        )

        # submitting the script to the cluster
        add_steps = EmrAddStepsOperator(
            task_id="add_steps",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.create_cluster', key='return_value') }}",
            aws_conn_id="aws_default",
            retries=0,
            steps=[ghcn_spark_step],
        )

        # watcher for ghcn
        watch_ghcn_step = EmrStepSensor(
            task_id="watch_ghcn_step",
            job_flow_id="{{ task_instance.xcom_pull('submit_spark_to_emr.create_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.add_steps', key='return_value')[0] }}",
            aws_conn_id="aws_default",
            retries=2,
            retry_delay=timedelta(seconds=15),
        )

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
        add_steps >> watch_ghcn_step
        watch_ghcn_step >> terminate_cluster

    # PART 4
    # creating the quality tests
    with TaskGroup(group_id="run_quality_tests") as run_quality_tests:

        # creating the quality tests
        # checking if there isn't data for the years selected
        DataQualityOperator(
            task_id="check_if_data_exists",
            s3fs_conn_id="{{ params.s3fs_conn_id }}",
            sql="""
                SELECT COUNT(*)
                FROM
                    (SELECT DISTINCT(year)
                    FROM ghcn) a
                RIGHT JOIN
                    (SELECT UNNEST([%s]) as year) b
                ON a.year = b.year
                WHERE a.year IS NULL
            """
            % ",".join(years),
            expected_result=0,
            error_message="The number of stored data for each year is not greater than 0!",
            register_s3_tables=[
                (
                    "ghcn",
                    "{{ params.s3_bucket }}/tables/ghcn/osm_adm8.parquet",
                    ["year"],
                )
            ],
        )

    # creating a symbolic task to show the DAG end
    end_operator = DummyOperator(task_id="stop_execution")

    # defining tasks relations
    start_operator >> [load_ghcn_data, load_script_files]
    [load_ghcn_data, load_script_files] >> submit_spark_to_emr
    submit_spark_to_emr >> run_quality_tests
    run_quality_tests >> end_operator
