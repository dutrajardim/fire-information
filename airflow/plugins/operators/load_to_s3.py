from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.decorators import apply_defaults
from hooks.s3fs import S3fsHook

from contextlib import closing
from urllib.request import Request, urlopen
import io
import os
import gzip
import zipfile


class LoadToS3Operator(BaseOperator):
    """
    Operator used to take data from remote address
    and load it to s3.
    """

    # defining operator box background color
    ui_color = "#7545a1"

    template_fields = ("pathname", "url", "s3fs_conn_id")

    @apply_defaults
    def __init__(
        self,
        s3fs_conn_id,
        url,
        pathname=None,
        pathname_callable=lambda x: x,
        gz_compress=False,
        unzip=False,
        headers={"User-Agent": "Mozilla/6.0"},
        unzip_filter=lambda filename: os.path.basename(filename),
        *args,
        **kwargs,
    ):
        """
        This function is responsible for instantiating the Operator.
        Either pathname or pathname_callable must be provided. First it will
        use pathname as address to store the data, and as it is not provided,
        pathname_callable will be used to get the path.

        Args:
            s3fs_conn_id (string): airflow s3 connection configuration
            url (satring): the remote address from where the data will be taken
            pathname (string, optional): path to s3 where data will be stored. Defaults to None.
            pathname_callable (lambda, optional): function that returns a pathname. Defaults to lambda x:x.
            gz_compress (bool, optional): flag to define if the data needs be compressed before saved to s3. Defaults to False.
            unzip (bool, optional): flag to define if is need to unzip the remote file before store it. Defaults to False.
            unzip_filter (lambda, optional): function that filter unzip files based in filename. Defaults to lambda filename: os.path.basename(filename).
        """

        # initializing inheritance
        super(LoadToS3Operator, self).__init__(*args, **kwargs)

        # defining operator properties
        self.s3fs_conn_id = s3fs_conn_id
        self.url = url
        self.pathname = pathname
        self.pathname_callable = pathname_callable
        self.gz_compress = gz_compress
        self.unzip = unzip
        self.unzip_filter = unzip_filter
        self.headers = headers

    def execute(self, context):
        """
        This will be executed as the operator is activated.

        Args:
            context (_type_): _description_
        """

        s3fs = S3fsHook(conn_id=self.s3fs_conn_id)
        fs = s3fs.get_filesystem()

        url = self.url
        self.log.info(f"Downloading from: {url}")
        req = Request(url, headers=self.headers)

        with closing(urlopen(req)) as resource:

            file_streams = []

            # if unzip is set to True, the src_stream will be
            # each of the file inside of the zip file
            # taken from the url
            if self.unzip:

                self.log.info("Getting files from zip archive.")
                in_stream = io.BytesIO(resource.read())
                zip_archive = zipfile.ZipFile(in_stream, "r")
                file_streams = [
                    (filename, zip_archive.open(filename))
                    for filename in zip_archive.namelist()
                    if self.unzip_filter(filename)
                ]

            # if unzip is set to False, the src_stream will be
            # just the file taken from the url
            else:
                file_streams.append((url, resource))

            # each of the src_streams will be load to s3
            for order, (filename, src_stream) in enumerate(file_streams):

                # if gz_compress is set to True, so we need
                # to compress it before save it
                if self.gz_compress:

                    self.log.info("Compressing files.")
                    out_stream = io.BytesIO()
                    gz_file = gzip.GzipFile(fileobj=out_stream, mode="wb")
                    gz_file.write(src_stream.read())
                    gz_file.close()

                    out_stream.seek(0)  # set pointer to beginning

                else:
                    out_stream = io.BytesIO(src_stream.read())

                # configging pathname with pathname arg
                # or with a callable
                if self.pathname:
                    s3_path = self.pathname
                else:
                    callable_params = {"url": url, "filename": filename, "order": order}
                    s3_path = self.pathname_callable(**callable_params)

                # saving the file
                s3_file = fs.open(s3_path, "wb")
                s3_file.write(out_stream.read())
                s3_file.close()

                out_stream.close()
                src_stream.close()
