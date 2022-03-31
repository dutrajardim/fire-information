import pyarrow.parquet as pq
import duckdb

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.s3fs import S3fsHook


class DataQualityOperator(BaseOperator):
    """
    Description:
        Operator used to run checks on the data.
    """

    # defining operator box background color
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self, s3fs_conn_id, dq_checks=[], register_s3_tables=[], *args, **kwargs
    ):

        # initializing inheritance
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        # defining operator properties
        self.dq_checks = dq_checks
        self.s3fs_conn_id = s3fs_conn_id
        self.register_s3_tables = register_s3_tables

    def execute(self, context):
        """
        Description:
            Execute a test query in the redshift warehouse and pass
            the response to the  python callable.
        """

        self.log.info("Running data quality checks...")
        s3fs = S3fsHook(conn_id=self.s3fs_conn_id)

        fs = s3fs.get_filesystem()
        con = duckdb.connect(database=":memory:")

        for table_name, s3_path in self.register_s3_tables:
            paths = fs.glob(s3_path)
            tmp_path = paths if len(paths) > 1 else paths[0]

            con.register(table_name, pq.read_table(tmp_path, filesystem=fs))

        # running tests for each test
        for order, check in enumerate(self.dq_checks):

            record = con.execute(check["sql"].format("arrow_table")).fetchone()
            self.log.info(
                "Data quality check of order %s returned the value %s."
                % (order + 1, record[0])
            )

            # checking for expected values
            if record[0] != check["expected_result"]:
                raise ValueError(check["error_message"])
