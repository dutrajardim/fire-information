from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.s3fs import S3fsHook

from contextlib import closing
import urllib.request
import io
import os
import gzip
import zipfile


class LoadToS3Operator(BaseOperator):
    """
    Description:
    """

    # defining operator box background color
    ui_color = "#7545a1"

    template_fields = ("pathname", "url")

    @apply_defaults
    def __init__(
        self,
        s3fs_conn_id,
        url,
        pathname=None,
        pathname_callable=lambda x: x,
        gz_compress=False,
        unzip=False,
        *args,
        **kwargs
    ):

        # initializing inheritance
        super(LoadToS3Operator, self).__init__(*args, **kwargs)

        # defining operator properties
        self.s3fs_conn_id = s3fs_conn_id
        self.url = url
        self.pathname = pathname
        self.pathname_callable = pathname_callable
        self.gz_compress = gz_compress
        self.unzip = unzip

    def execute(self, context):
        """
        Description:
        """

        s3fs = S3fsHook(conn_id=self.s3fs_conn_id)
        fs = s3fs.get_filesystem()

        url = self.url

        with closing(urllib.request.urlopen(url)) as resource:

            file_streams = []

            # if unzip is set to True, the src_stream will be
            # each of the file inside of the zip file
            # taken from the url
            if self.unzip:
                in_stream = io.BytesIO(resource.read())
                zip_archive = zipfile.ZipFile(in_stream, "r")
                file_streams = [
                    (filename, zip_archive.open(filename))
                    for filename in zip_archive.namelist()
                    if os.path.basename(filename)
                ]

            # if unzip is set to False, the src_stream will be
            # just the file taken from the url
            else:
                file_streams.append((0, url, resource))

            # each of the src_streams will be load to s3
            for order, (filename, src_stream) in enumerate(file_streams):

                # if gz_compress is set to True, so we need
                # to compress it before save it
                if self.gz_compress:
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
