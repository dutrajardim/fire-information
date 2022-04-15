import functools
import copy
from airflow.hooks.base import BaseHook

# default values for emr job
JOB_FLOW_OVERRIDES = {
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m3.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m3.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,  # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# fmt: off
# default values for emr steps
SPARK_STEPS =  {
    "HadoopJarStep": {
        'Jar': 'command-runner.jar',
        "Args": [
            "spark-submit", "--master", "yarn", "--deploy-mode", "client",
            "--conf", "spark.jars.packages=org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.1-incubating,org.datasyslab:geotools-wrapper:1.1.0-25.2",
            "--conf", "spark.hadoop.fs.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
            "--conf", "spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator",
            "--conf", "spark.kryoserializer.buffer.max=512",
        ],
    },
}
# fmt: on


class EmrTemplates:
    """
    This class define helper functions
    to create Amazon EMR jobs and tasks.
    """

    def get_job_flow_overrides(
        self,
        ec2_subnet_id=None,
        key_name=None,
        bootstrap_actions=None,
        log_uri="s3://dutrajardim-logs/emr",
        emr_release="emr-6.5.0",
        applications=[{"Name": "Spark"}],
        name="Fire Information Project",
    ):
        """
        Helper for create new Amazon EMR Job

        Args:
            ec2_subnet_id (str, optional): Set this parameter to the identifier of the Amazon VPC subnet where you want the cluster to launch. Defaults to None.
            key_name (str, optional): The name of the Amazon EC2 key pair. Defaults to None.
            bootstrap_actions (list, optional): A list of the bootstrap actions run by the job flow. Defaults to None.
            log_uri (str, optional): The location in Amazon S3 to write the log files of the job flow.. Defaults to "s3://dutrajardim-logs/emr".
            emr_release (str, optional): The Amazon EMR release label. Defaults to "emr-6.5.0".
            applications (list, optional): List of applications for Amazon EMR to install and configure when launching the cluster. Defaults to [{"Name": "Spark"}].
            name (str, optional): The name of the job. Defaults to "Fire Information Project".

        Returns:
            dict: Template to be used to create the Job Flow
        """
        template = copy.deepcopy(JOB_FLOW_OVERRIDES)

        template["Instances"]["Ec2SubnetId"] = ec2_subnet_id
        template["Instances"]["Ec2KeyName"] = key_name
        template["BootstrapActions"] = bootstrap_actions
        template["Name"] = name
        template["LogUri"] = log_uri
        template["ReleaseLabel"] = emr_release
        template["Applications"] = applications

        return template

    def get_spark_step(
        self,
        script_path,
        py_files=None,
        arguments=[],
        name="Default Name",
        action_on_failure="CANCEL_AND_WAIT",
    ):
        """
        Helper for create EMR step.

        Args:
            script_path (str): S3 path of the main script.
            py_files (str, optional): S3 path of zip file containing the main script's python dependencies. Defaults to None.
            arguments (list, optional): Arguments to be passed for the main script. Defaults to [].
            name (str, optional): A name to identify the step. Defaults to "Default Name".
            action_on_failure (str, optional): The action to take when the step fails. Defaults to "CANCEL_AND_WAIT".

        Returns:
            dict: Template to be used to create the step.
        """
        template = copy.deepcopy(SPARK_STEPS)

        template["Name"] = name
        template["ActionOnFailure"] = action_on_failure

        if py_files:
            template["HadoopJarStep"]["Args"].extend(["--py-files", py_files])

        template["HadoopJarStep"]["Args"].append(script_path)

        for argv in arguments:
            template["HadoopJarStep"]["Args"].extend(argv)

        return template
