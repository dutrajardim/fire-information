from minio import Minio
from contextlib import closing
import urllib3
import urllib.request
import io
import gzip
import zipfile


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
        This function is responsible for parse a string
        with the name of file and year of the data that
        will be used to format a url string to download
        data from Earth Data (NASA).
    Arguments:
        None.
    Returns:
        None.
    """

    files_str = "DL_FIRE_SV-C2_251209.zip:2018, DL_FIRE_SV-C2_251208.zip:2019, DL_FIRE_SV-C2_251207.zip:2020, DL_FIRE_SV-C2_249334.zip:2021"
    url_str = "https://firms.modaps.eosdis.nasa.gov/data/download/%s"

    files = [file.split(":") for file in files_str.replace(" ", "").split(",")]
    map_files = lambda fn, yr: [url_str % fn, yr[0] if yr else fn.split(".")[0]]

    return [map_files(fn, yr) for fn, *yr in files]


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
        s3_path = "src/firms/suomi_viirs_c2/archive/%s.csv.gz"%basename # configuring s3 path

        # setting the data stream
        # fmt: off
        with \
            closing(urllib.request.urlopen(url)) as resource, \
            closing(io.BytesIO(resource.read())) as in_stream, \
            closing(zipfile.ZipFile(in_stream, "r")) as zip_file, \
            closing(io.BytesIO()) as out_stream:
            # fmt: on

            # checking  for csv file in the zip file (only one csv is expected)
            filenames = [fl for fl in zip_file.namelist() if fl.endswith(".csv")]
            filename = filenames[0]

            # writing to csv.gz
            gz_file = gzip.GzipFile(fileobj=out_stream, mode="wb")
            gz_file.write(zip_file.read(filename))
            gz_file.close()
            
            file_size = out_stream.getbuffer().nbytes  # check file zise
            out_stream.seek(0)  # set pointer to beginning
            m_client.put_object(s3_bucket, s3_path, out_stream, file_size)  # saving to s3


if __name__ == "__main__":
    main()
