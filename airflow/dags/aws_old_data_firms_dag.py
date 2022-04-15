"""
# Firms Files ETL

This DAG is responsible for loading remote old FIRMS data
from nasa to s3 and then create parquet table defining in which
administrative area the fire spot occurred.
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
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}

# creating the DAG
with DAG(
    "aws_old_data_firms_dag",
    default_args=default_args,
    description=(
        "Load remote old FIRMS data from nasa "
        "to s3 and then create parquet table defining in which"
        "administrative area the fire spot occurred."
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

    dag.__dict__

    # creating a symbolic task to show the DAG begin
    start_operator = DummyOperator(task_id="begin_execution")

    # PART 1
    # Creating tasks to load data to s3
    with TaskGroup(group_id="load_old_firms_data") as load_old_firms_data:

        for year in years:

            # function to create the path name
            # where data will be stored in s3
            # based in zip content file name
            pathname_callable = (
                lambda filename, params, year=year, **kwargs: "%s/src/firms/suomi_viirs_c2/archive/%s/%s.gz"
                % (params["s3_bucket"], year, os.path.basename(filename))
            )

            # loading the file to s3
            LoadToS3Operator(
                task_id=f"load_file_{year}",
                s3fs_conn_id="{{ params.s3fs_conn_id }}",
                url=f"https://firms.modaps.eosdis.nasa.gov/data/country/zips/viirs-snpp_{year}_all_countries.zip",
                pathname_callable=pathname_callable,
                unzip=True,
                gz_compress=True,
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
        for filename in ["firms_spark_etl.py", "bootstrap-actions.sh"]:

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

        # This extracts s3 firms data loaded from nasa to s3,
        # makes a spatial join with geo shapes of administrative areas and
        # then stores the result back to S3.
        firms_spark_step = emr_templates.get_spark_step(
            name="Spatial join of FIRMS with geo shapes of administrative areas",
            script_path="s3a://{{ params.s3_bucket }}/spark_scripts/firms_spark_etl.py",
            arguments=[
                (
                    "--s3-firms-src-path",
                    "s3a://%s/src/firms/suomi_viirs_c2/archive/{%s}"
                    % ("{{ params.s3_bucket }}", ",".join(years)),
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

        # submitting the script to the cluster
        add_steps = EmrAddStepsOperator(
            task_id="add_steps",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='submit_spark_to_emr.create_cluster', key='return_value') }}",
            aws_conn_id="aws_default",
            retries=0,
            steps=[firms_spark_step],
        )

        # watcher for firms
        watch_firms_step = EmrStepSensor(
            task_id="watch_firms_step",
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
        add_steps >> watch_firms_step
        watch_firms_step >> terminate_cluster

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
                    FROM firms) a
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
                    "firms",
                    "{{ params.s3_bucket }}/tables/firms/osm_adm8.parquet",
                    ["year"],
                )
            ],
        )

    # creating a symbolic task to show the DAG end
    end_operator = DummyOperator(task_id="end_execution")

    # defining tasks relations
    start_operator >> [load_old_firms_data, load_script_files]
    [load_old_firms_data, load_script_files] >> submit_spark_to_emr
    submit_spark_to_emr >> run_quality_tests
    run_quality_tests >> end_operator
