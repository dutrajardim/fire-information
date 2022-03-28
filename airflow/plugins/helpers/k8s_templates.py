from airflow.hooks.base import BaseHook


class K8sTemplates:
    """
    Description:
    """

    default_template = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {},
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

    def __init__(self, s3fs_conn_id):
        self.s3fs_conn_id = s3fs_conn_id

    def get_stations_application_template(
        self,
        name="stations-spark-script",
        namespace="spark-apps",
        main_application_file="s3a://dutrajardim-fi/spark_scripts/stations_spark_etl.py",
        spark_app_name="DJ - Station Information",
    ):

        s3_conn = BaseHook.get_connection(self.s3fs_conn_id)

        body = self.default_template

        body["metadata"]["namespace"] = namespace
        body["metadata"]["name"] = name
        body["spec"]["mainApplicationFile"] = main_application_file
        body["spec"]["sparkConf"]["spark.app.name"] = spark_app_name

        body["spec"]["hadoopConf"]["fs.s3a.endpoint"] = s3_conn.host
        body["spec"]["hadoopConf"]["fs.s3a.access.key"] = s3_conn.login
        body["spec"]["hadoopConf"]["fs.s3a.secret.key"] = s3_conn.password

        return body
