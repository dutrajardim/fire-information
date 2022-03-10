from minio import Minio
from contextlib import closing
import urllib3
import urllib
import io
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


def main():
    m_client = get_minio_client()

    # countries
    countries_str = "BRA, ARG, PRY, URY"
    countries = countries_str.replace(" ", "").split(",")

    url_str = "https://geodata.ucdavis.edu/gadm/gadm4.0/shp/gadm40_{}_shp.zip"
    urls = map(lambda x: url_str.format(x), countries)

    s3_bucket = "dutrajardim-fi"

    for url in urls:
        with closing(urllib.request.urlopen(url)) as resource:
            stream = io.BytesIO(resource.read())

            with closing(zipfile.ZipFile(stream, "r")) as zip_archive:
                for filename in zip_archive.namelist():

                    basename = os.path.basename(filename)
                    name, _ = os.path.splitext(basename)
                    version, iso_country, adm_level = name.split("_")

                    file_stream = io.BytesIO(zip_archive.open(filename).read())
                    file_size = file_stream.getbuffer().nbytes

                    s3_object_key = "src/shapes/%s/adm_%s/%s/%s".format(
                        version, adm_level, iso_country, basename
                    )

                    m_client.put_object(
                        s3_bucket, s3_object_key, file_stream, file_size
                    )
