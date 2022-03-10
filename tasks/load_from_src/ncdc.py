from minio import Minio
from contextlib import closing
import urllib3
import urllib.request
import io


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

def parse_file_list():
    """
    Description:
        None.
    Arguments:
        None.
    Returns:
        None.
    """

    files_str = "2018, 2019, 2020, 2021"
    url_str = "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/%s.csv.gz"
    
    return [[url_str % file, file] for file in files_str.replace(" ", "").split(",")]

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
    urls = parse_file_list()

    s3_bucket = "dutrajardim-fi"

    for url, basename in urls:
        s3_path = "src/ncdc/ghcn/%s.csv.gz"%basename # configuring s3 path

        # fmt: off
        with \
            closing(urllib.request.urlopen(url)) as resource, \
            closing(io.BytesIO(resource.read())) as raw_data:
        # fmt: on

            raw_data_size = raw_data.getbuffer().nbytes
            m_client.put_object(s3_bucket, s3_path, raw_data, raw_data_size)

   
if __name__ == "__main__":
    main()
