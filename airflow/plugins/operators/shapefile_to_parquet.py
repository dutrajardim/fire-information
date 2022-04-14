# WORK IN PROGRESS


# import pyarrow.parquet as pq
# import pyarrow as pa
# import pygeoif
# import duckdb
# import shapefile
# import functools
# import io
# import os

# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults
# from hooks.s3fs import S3fsHook


# class ShapefileToParquetOperator(BaseOperator):
#     """
#     Description:
#     """

#     # defining operator box background color
#     ui_color = "#5DB9B4"

#     @apply_defaults
#     def __init__(
#         self,
#         s3fs_conn_id,
#         path_shp,
#         path_pq,
#         fields={},
#         partition_cols=[],
#         select_expr=None,
#         partition_filename_cb=lambda x, name: "%s.snappy.parquet" % name,
#         *args,
#         **kwargs
#     ):

#         # initializing inheritance
#         super(ShapefileToParquetOperator, self).__init__(*args, **kwargs)

#         # defining operator properties
#         self.s3fs_conn_id = s3fs_conn_id
#         self.path_shp = path_shp
#         self.path_pq = path_pq
#         self.fields = fields
#         self.partition_cols = partition_cols
#         self.select_expr = select_expr
#         self.partition_filename_cb = partition_filename_cb

#     def execute(self, context):
#         """
#         Description:
#         """

#         self.log.info("Shapefile to parquet")
#         s3fs = S3fsHook(conn_id=self.s3fs_conn_id)
#         fs = s3fs.get_filesystem()

#         file_paths = self._group_filepaths(fs)

#         for path in file_paths:

#             pa_records = self._shapefile_to_pyarrow(path, fs)

#             if self.select_expr:
#                 con = duckdb.connect()
#                 pa_records = con.execute(
#                     "SELECT %s FROM pa_records" % ",".join(self.select_expr)
#                 ).arrow()

#             basename = os.path.basename(path)
#             cb = lambda x, func=self.partition_filename_cb, name=basename: func(x, name)
#             pq.write_to_dataset(
#                 pa_records,
#                 root_path=self.path_pq,
#                 partition_cols=self.partition_cols,
#                 compression="SNAPPY",
#                 flavor="spark",
#                 filesystem=fs,
#                 partition_filename_cb=cb,
#             )

#     def _group_filepaths(self, fs):

#         return functools.reduce(
#             lambda acc, cur: acc | {os.path.splitext(cur)[0]},
#             fs.glob(self.path_shp),
#             set(),
#         )

#     def _shapefile_to_pyarrow(self, shapefile_path, fs):
#         sf = shapefile.Reader(
#             shp=io.BytesIO(fs.open(shapefile_path + ".shp").read()),
#             shx=io.BytesIO(fs.open(shapefile_path + ".shx").read()),
#             dbf=io.BytesIO(fs.open(shapefile_path + ".dbf").read()),
#         )

#         sf_fields = [x[0] for x in sf.fields[1:]]
#         fields = self.fields if self.fields else sf_fields

#         def map_record(sr):
#             sr_dict = {"geometry": pygeoif.as_shape(sr.shape).wkt}
#             return functools.reduce(
#                 lambda acc, cur: dict({**acc, fields[cur[0]]: cur[1]}),
#                 enumerate(sr.record),
#                 sr_dict,
#             )

#         pylist = [
#             map_record(shape_record)
#             for shape_record in sf.iterShapeRecords(fields=fields)
#         ]

#         return pa.Table.from_pylist(pylist)
