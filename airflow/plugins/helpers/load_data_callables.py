from hooks.s3fs import S3fsHook

from contextlib import closing
import urllib.request
import io
import os
import gzip
import zipfile

class LoadDataCallables:

    def __init__(self, s3fs_conn_id):
        self.s3fs_conn_id = s3fs_conn_id

    
    def nasa_firms(self):

        s3fs = S3fsHook(conn_id=self.s3fs_conn_id)
        fs = s3fs.get_filesystem()

        files_str = "DL_FIRE_SV-C2_251209.zip:2018, DL_FIRE_SV-C2_251208.zip:2019, DL_FIRE_SV-C2_251207.zip:2020, DL_FIRE_SV-C2_249334.zip:2021"
        url_str = "https://firms.modaps.eosdis.nasa.gov/data/download/%s"

        files = [file.split(":") for file in files_str.replace(" ", "").split(",")]
        map_files = lambda fn, yr: [url_str % fn, yr[0] if yr else fn.split(".")[0]]

        urls = [map_files(fn, yr) for fn, *yr in files]

        for url, basename in urls:
            s3_path = "dutrajardim-fi/src/firms/suomi_viirs_c2/archive/%s.csv.gz"%basename # configuring s3 path

            # setting the data stream
            # fmt: off
            with \
                closing(urllib.request.urlopen(url)) as resource, \
                closing(io.BytesIO(resource.read())) as in_stream, \
                closing(zipfile.ZipFile(in_stream, "r")) as zip_file, \
                closing(io.BytesIO()) as out_stream, \
                closing(fs.open(s3_path, 'wb')) as s3_file:
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
                s3_file.write(out_stream.read()) # saving to s3


    def ncdc(self, urls, file_path_callable, gz_compress=False):
        """
        Description:
            None.
        Arguments:
            None.
        Returns:
            None.
        """
        conn_id = self.s3fs_conn_id

        def callable():
            s3fs = S3fsHook(conn_id=conn_id)
            fs = s3fs.get_filesystem()

            for order, url in enumerate(urls):

                resource = urllib.request.urlopen(url)

                callable_params = {"url": url,"order": order}
                s3_path = file_path_callable(**callable_params) # configuring s3 path

                if gz_compress:
                    out_stream = io.BytesIO()
                    gz_file = gzip.GzipFile(fileobj=out_stream, mode="wb")
                    gz_file.write(resource.read())
                    gz_file.close()
                    
                    out_stream.seek(0)  # set pointer to beginning
                
                else:
                    out_stream = io.BytesIO(resource.read())

                s3_file = fs.open(s3_path, 'wb')
                s3_file.write(out_stream.read())
                s3_file.close()

                out_stream.close()
                resource.close()

        return callable


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
            
            out_stream.seek(0)  # set pointer to beginning
            s3_file.write(out_stream.read())  # saving to s3


    def gadm_shapes(self):

        s3fs = S3fsHook(conn_id=self.s3fs_conn_id)
        fs = s3fs.get_filesystem()

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