from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.kubernetes import KubernetesHook

import kubernetes.watch
import datetime
import json


class SparkOnK8sAppOperator(BaseOperator):
    """
    Description:
    """

    # defining operator box background color
    ui_color = "#F39480"

    @apply_defaults
    def __init__(
        self,
        template,
        k8s_conn_id,
        timeout_seconds=0,
        wait_timeout_seconds=25,
        *args,
        **kwargs
    ):

        # initializing inheritance
        super(SparkOnK8sAppOperator, self).__init__(*args, **kwargs)

        # defining operator properties
        self.k8s_conn_id = k8s_conn_id
        self.custom_object_name = template["metadata"]["name"]
        self.timeout_seconds = timeout_seconds
        self.wait_timeout_seconds = wait_timeout_seconds

        group, version = template["apiVersion"].split("/")
        self.custom_object_params = {
            "group": group,
            "version": version,
            "namespace": template["metadata"]["namespace"],
            "plural": "sparkapplications",
        }
        self.template = template

    def execute(self, context):
        """
        Description:
        """
        k8s_hook = KubernetesHook(conn_id=self.k8s_conn_id)
        custom_object_api = k8s_hook.get_custom_object_api()

        watch = kubernetes.watch.Watch()

        args = {
            "field_selector": "metadata.name=%s" % self.custom_object_name,
            **self.custom_object_params,
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
                            **self.custom_object_params, name=self.custom_object_name
                        )

        # creating object
        custom_object_api.create_namespaced_custom_object(
            **self.custom_object_params, body=self.template
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
