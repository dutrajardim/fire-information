from airflow.hooks.base import BaseHook

import kubernetes.client
import kubernetes.config
import tempfile
import json


class KubernetesHook(BaseHook):
    """
    This hook is responsible for kubernetes
    api interations.
    """

    api_client = None

    def __init__(self, conn_id, *args, **kwargs):
        """
        This function is responsible for instantiating a
        KubernetesHook object.

        Args:
            conn_id (str): id of the airflow connection (Kubernetes connection)
        """

        # initializing inheritance
        super(KubernetesHook, self).__init__(*args, **kwargs)

        # defining hook properties
        self.conn = self.get_connection(conn_id)

        # save configurations to a temporally local file
        # and then load kube config from it
        tf = tempfile.NamedTemporaryFile()
        extras = self.conn.extra_dejson
        tf.write(extras.get("extra__kubernetes__kube_config").encode())
        tf.flush()

        kubernetes.config.load_kube_config(tf.name)
        self.api_client = kubernetes.client.ApiClient()
        tf.close()

    def get_api_client(self):
        """
        This function is responsible for exposing
        kubernetes client

        Returns:
            kubernetes.client.ApiClient: connection to the kubernetes cluster
        """
        return self.api_client

    def get_custom_object_api(self):
        """
        This function is responsible for exposing
        kubernetes' custom objects api

        Returns:
            kubernetes.client.CustomObjectsApi: client of kubernetes' custom objects api
        """
        return kubernetes.client.CustomObjectsApi(self.api_client)
