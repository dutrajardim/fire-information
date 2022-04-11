import functools
import copy
from airflow.hooks.base import BaseHook

JOB_FLOW_OVERRIDES = {
    "Name": "Fire Information Project",
    "ReleaseLabel": "emr-6.5.0",
    "LogUri": "s3://dutrajardim-logs/emr",
    "Applications": [{"Name": "Spark"}],
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
    def get_job_flow_overrides(
        self, ec2_subnet_id=None, key_name=None, bootstrap_actions=None
    ):
        template = copy.deepcopy(JOB_FLOW_OVERRIDES)

        if ec2_subnet_id:
            template["Instances"]["Ec2SubnetId"] = ec2_subnet_id

        if key_name:
            template["Instances"]["Ec2KeyName"] = key_name

        if bootstrap_actions:
            template["BootstrapActions"] = bootstrap_actions

        return template

    def get_spark_step(
        self,
        script_path,
        py_files=None,
        arguments=[],
        name="Default Name",
        action_on_failure="CANCEL_AND_WAIT",
    ):
        template = copy.deepcopy(SPARK_STEPS)

        template["Name"] = name
        template["ActionOnFailure"] = action_on_failure

        if py_files:
            template["HadoopJarStep"]["Args"].extend(["--py-files", py_files])

        template["HadoopJarStep"]["Args"].append(script_path)

        for argv in arguments:
            template["HadoopJarStep"]["Args"].extend(argv)

        return template
