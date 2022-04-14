import s3fs

from airflow.hooks.base import BaseHook


class S3fsHook(BaseHook):
    """
    This hook is responsible for s3
    filesystem interactions
    """

    def __init__(self, conn_id, *args, **kwargs):
        """
        This functions is responsible for instantiating a S3fsHook object.
        It makes use of the follow airflow s3 connection params: host, login and password.
        Others configuration can be setted in the extra param of the airflow connection.

        Args:
            conn_id (str): airflow connection of the type s3
        """

        # initializing inheritance
        super(S3fsHook, self).__init__(*args, **kwargs)

        # defining hook properties
        self.conn = self.get_connection(conn_id)

    def get_filesystem(self):
        """
        This function is responsible for exposing
        a filesystem-like (ls, cp, open) on top of S3 storage.

        Returns:
            s3fs.S3FileSystem: filesystem-like
        """

        # configging
        client_kwargs = {
            "endpoint_url": self.conn.host,
            "aws_access_key_id": self.conn.login,
            "aws_secret_access_key": self.conn.password,
        }

        # checking for configuration arguments
        if self.conn.extra and "client_kwargs" in self.conn.extra_dejson:
            client_kwargs = {**client_kwargs, **self.conn.extra_dejson["client_kwargs"]}

        return s3fs.S3FileSystem(client_kwargs=client_kwargs)
