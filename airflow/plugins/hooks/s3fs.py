import s3fs

from airflow.hooks.base import BaseHook


class S3fsHook(BaseHook):
    def __init__(self, conn_id, *args, **kwargs):
        super(S3fsHook, self).__init__(*args, **kwargs)
        self.conn = self.get_connection(conn_id)

    def get_filesystem(self):
        client_kwargs = {
            "endpoint_url": self.conn.host,
            "aws_access_key_id": self.conn.login,
            "aws_secret_access_key": self.conn.password,
        }

        if self.conn.extra and "client_kwargs" in self.conn.extra_dejson:
            client_kwargs = {**client_kwargs, **self.conn.extra_dejson["client_kwargs"]}

        return s3fs.S3FileSystem(client_kwargs=client_kwargs)
