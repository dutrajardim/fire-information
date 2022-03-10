from minio import Minio
from contextlib import closing
import urllib3
import urllib.request
import io
import gzip


def get_minio_client():
    """
    Description:
        None.
    Arguments:
        None.
    Returns:
        None.
    """

    http_client = urllib3.PoolManager(
        cert_reqs="CERT_REQUIRED", ca_certs="/home/rafael/.certs/dev/rootCA.pem"
    )

    m_client = Minio(
        "minio.minio-tenant",
        access_key="admin",
        secret_key="6bd71ace-8866-407a-9bcc-714bc5753f18",
        http_client=http_client,
    )

    return m_client


def main():
    """
    Description:
        None.
    Arguments:
        None.
    Returns:
        None.
    """

    m_client = get_minio_client()

    s3_bucket = "dutrajardim-fi"
    s3_path = "src/ncdc/stations.txt.gz"
    url = "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"

    # setting the data stream
    # fmt: off
    with \
        closing(urllib.request.urlopen(url)) as resource, \
        closing(io.BytesIO()) as out_stream:
        # fmt: on

        # writing to txt.gz
        gz_file = gzip.GzipFile(fileobj=out_stream, mode="wb")
        gz_file.write(resource.read())
        gz_file.close()
        
        file_size = out_stream.getbuffer().nbytes  # check file zise
        out_stream.seek(0)  # set pointer to beginning
        m_client.put_object(s3_bucket, s3_path, out_stream, file_size)  # saving to s3


if __name__ == "__main__":
    main()
