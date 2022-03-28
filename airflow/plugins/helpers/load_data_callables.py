from hooks.s3fs import S3fsHook

from contextlib import closing
import urllib.request
import io
import gzip

class LoadDataCallables:

    def __init__(self, s3fs_conn_id):
        self.s3fs_conn_id = s3fs_conn_id

    def ncdc_stations(self):
        """
        Description:
            None.
        Arguments:
            None.
        Returns:
            None.
        """

        s3fs = S3fsHook(conn_id=self.s3fs_conn_id)
        fs = s3fs.get_filesystem()

        s3_path = "dutrajardim-fi/src/ncdc/stations.txt.gz"
        url = "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"

        # setting the data stream
        # fmt: off
        with \
            closing(urllib.request.urlopen(url)) as resource, \
            closing(io.BytesIO()) as out_stream, \
            closing(fs.open(s3_path, 'wb')) as s3_file:
            # fmt: on

            # writing to txt.gz
            gz_file = gzip.GzipFile(fileobj=out_stream, mode="wb")
            gz_file.write(resource.read())
            gz_file.close()
            
            file_size = out_stream.getbuffer().nbytes  # check file zise
            out_stream.seek(0)  # set pointer to beginning
            s3_file.write(out_stream.read())  # saving to s3


    def gadm_shapes(self):

        s3fs = S3fsHook(conn_id=self.s3fs_conn_id)
        fs = s3fs.get_filesystem()
        # m_client = get_minio_client()

        # countries
        countries_str = "BRA, ARG, PRY, URY"
        countries = countries_str.replace(" ", "").split(",")

        url_str = "https://geodata.ucdavis.edu/gadm/gadm4.0/shp/gadm40_{}_shp.zip"
        urls = map(lambda x: url_str.format(x), countries)

        s3_bucket = "dutrajardim-fi"

        for url in urls:

            with \
                closing(urllib.request.urlopen(url)) as resource, \
                closing(io.BytesIO(resource.read())) as in_stream, \
                closing(zipfile.ZipFile(in_stream, "r")) as zip_archive:

                for filename in zip_archive.namelist():

                    basename = os.path.basename(filename)
                    name, _ = os.path.splitext(basename)
                    version, iso_country, adm_level = name.split("_")

                    s3_path = "dutrajardim-fi/src/shapes/%s/adm_%s/%s/%s" % (
                        version, adm_level, iso_country, basename
                    )

                    with \
                        closing(io.BytesIO(zip_archive.open(filename).read())) as out_stream, \
                        closing(fs.open(s3_path, 'wb')) as s3_file:

                        s3_file.write(out_stream.read())