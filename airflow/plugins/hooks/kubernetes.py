from airflow.hooks.base import BaseHook

import kubernetes.client
import kubernetes.config
import tempfile
import json


class KubernetesHook(BaseHook):
    api_client = None

    def __init__(self, conn_id, *args, **kwargs):
        super(KubernetesHook, self).__init__(*args, **kwargs)
        self.conn = self.get_connection(conn_id)

        tf = tempfile.NamedTemporaryFile()

        extras = self.conn.extra_dejson
        tf.write(extras.get("extra__kubernetes__kube_config").encode())
        tf.flush()

        kubernetes.config.load_kube_config(tf.name)
        self.api_client = kubernetes.client.ApiClient()
        tf.close()

    def get_api_client(self):
        return self.api_client

    def get_custom_object_api(self):
        return kubernetes.client.CustomObjectsApi(self.api_client)
