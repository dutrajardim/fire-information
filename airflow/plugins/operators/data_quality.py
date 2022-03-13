import logging
import s3fs
import pyarrow.parquet as pq
import duckdb

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Description:
        Operator used to run checks on the data.
    """

    # defining operator box background color
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, dq_checks=[], *args, **kwargs):

        # initializing inheritance
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        # defining operator properties
        self.dq_checks = dq_checks

    def execute(self, context):
        """
        Description:
            Execute a test query in the redshift warehouse and pass
            the response to the  python callable.
        """

        self.log.info("Running data quality checks...")
        fs = s3fs.S3FileSystem(
            client_kwargs={
                "endpoint_url": "https://minio.minio-tenant",
                "aws_access_key_id": "admin",
                "aws_secret_access_key": "6bd71ace-8866-407a-9bcc-714bc5753f18",
                "verify": False,
            }
        )

        # running tests for each test
        for order, check in enumerate(self.dq_checks):
            arrow_table = pq.read_table(check["s3_table"], filesystem=fs)
            con = duckdb.connect()

            record = con.execute(check["check_sql"].format("arrow_table")).fetchone()
            self.log.info(
                "Data quality check of order {} returned the value {}.".format(
                    order + 1, record[0]
                )
            )

            # checking for expected value
            if record[0] != check["expected_result"]:
                raise ValueError(check["error_message"])
