from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.kubernetes import KubernetesHook
from airflow.hooks.base import BaseHook

import kubernetes.watch
import datetime
import json
import functools


class SparkOnK8sAppOperator(BaseOperator):
    """
    Description:
    """

    # defining operator box background color
    ui_color = "#F39480"

    template_fields = ("s3fs_conn_id", "arguments", "main_application_file")

    @apply_defaults
    def __init__(
        self,
        name,
        main_application_file,
        k8s_conn_id,
        spark_app_name="DJ - Spark Application",
        s3fs_conn_id="",
        envs=[],
        arguments=[],
        timeout_seconds=0,
        wait_timeout_seconds=25,
        log_path="s3a://spark-logs/events",
        *args,
        **kwargs
    ):

        # initializing inheritance
        super(SparkOnK8sAppOperator, self).__init__(*args, **kwargs)

        # defining others properties
        self.name = name
        self.main_application_file = main_application_file
        self.spark_app_name = spark_app_name
        self.log_path = log_path
        self.arguments = arguments
        self.k8s_conn_id = k8s_conn_id
        self.timeout_seconds = timeout_seconds
        self.wait_timeout_seconds = wait_timeout_seconds
        self.s3fs_conn_id = s3fs_conn_id

    def _template_config(self):

        self.template["spec"]["arguments"] = functools.reduce(
            lambda acc, cur: [*acc, *cur], self.arguments, []
        )

        self.template["metadata"]["name"] = self.name
        self.template["spec"]["mainApplicationFile"] = self.main_application_file
        self.template["spec"]["sparkConf"]["spark.app.name"] = self.spark_app_name
        self.template["spec"]["sparkConf"]["spark.eventLog.dir"] = self.log_path

        if self.s3fs_conn_id:
            s3_conn = BaseHook.get_connection(self.s3fs_conn_id)

            self.template["spec"]["hadoopConf"]["fs.s3a.endpoint"] = s3_conn.host
            self.template["spec"]["hadoopConf"]["fs.s3a.access.key"] = s3_conn.login
            self.template["spec"]["hadoopConf"]["fs.s3a.secret.key"] = s3_conn.password

    def execute(self, context):
        """
        Description:
        """

        self._template_config()

        k8s_hook = KubernetesHook(conn_id=self.k8s_conn_id)
        custom_object_api = k8s_hook.get_custom_object_api()

        watch = kubernetes.watch.Watch()

        custom_object_name = self.template["metadata"]["name"]
        group, version = self.template["apiVersion"].split("/")
        custom_object_params = {
            "group": group,
            "version": version,
            "namespace": self.template["metadata"]["namespace"],
            "plural": "sparkapplications",
        }

        args = {
            "field_selector": "metadata.name=%s" % custom_object_name,
            **custom_object_params,
        }

        resp = custom_object_api.list_namespaced_custom_object(**args)
        args["func"] = custom_object_api.list_namespaced_custom_object
        args["timeout_seconds"] = self.wait_timeout_seconds

        # checking if there is any running object that match the new obj specification
        if len(resp["items"]):

            # deleting completed objects
            for event in watch.stream(**args):
                if event["type"] == "DELETED":
                    watch.stop()

                elif "status" in event["object"]:
                    state = event["object"]["status"]["applicationState"]["state"]

                    if state in ["FAILED", "SUBMISSION_FAILED", "COMPLETED"]:
                        custom_object_api.delete_namespaced_custom_object(
                            **custom_object_params, name=custom_object_name
                        )

        # creating object
        custom_object_api.create_namespaced_custom_object(
            **custom_object_params, body=self.template
        )

        args["timeout_seconds"] = self.timeout_seconds

        for event in watch.stream(**args):

            cur_time = datetime.datetime.now().isoformat()
            name = event["object"]["metadata"]["name"]
            log_msg = "Resource %s %s: %s" % (event["type"], cur_time, name)

            self.log.info(log_msg)

            if "status" in event["object"]:
                status = event["object"]["status"]
                state = status["applicationState"]["state"]

                log_json = json.dumps(status, indent=2)
                self.log.info("New state reported: %s" % log_json)

                if state in ["FAILED", "SUBMISSION_FAILED", "COMPLETED"]:
                    watch.stop()

                    if state != "COMPLETED":

                        error_msg = "Spark application does not \
                                completed without errors"
                        raise Exception(error_msg)

    template = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"namespace": "spark-apps", "name": "spark-script"},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": "dutradocker/spark-py:3.2.0",
            "imagePullPolicy": "Always",
            "sparkVersion": "3.2.0",
            "sparkConf": {
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": "s3a://spark-logs/events",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
                "spark.kryoserializer.buffer.max": "512",
                "spark.jars.ivy": "/tmp/ivy",
            },
            "hadoopConf": {
                "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "fs.s3a.path.style.access": "true",
                "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            },
            "restartPolicy": {
                "type": "OnFailure",
                "onFailureRetries": 1,
                "onSubmissionFailureRetries": 1,
            },
            "driver": {
                "serviceAccount": "spark",
                "cores": 1,
                "memory": "4g",
                "labels": {"version": "3.2.0"},
            },
            "executor": {
                "serviceAccount": "spark",
                "cores": 1,
                "instances": 2,
                "memory": "4g",
                "labels": {"version": "3.2.0"},
            },
        },
    }
