import pyarrow.parquet as pq
import duckdb

from airflow.models import BaseOperator
from hooks.s3fs import S3fsHook


class DataQualityOperator(BaseOperator):
    """
    Operator used to run quality checks on the data.
    """

    # defining operator box background color
    ui_color = "#89DA59"

    template_fields = ("register_s3_tables", "sql", "s3fs_conn_id")

    def __init__(
        self,
        s3fs_conn_id,
        sql,
        error_message="Error during quality test",
        expected_result=1,
        register_s3_tables=[],
        *args,
        **kwargs,
    ):
        """
        This function is responsible for instantiating a DataQualityOperator object.
        As the operator object is executed, the sql query result
        will be validated with expected result arg.

        register_s3_tables example:
        [
            ("ex_table", "my-s3-bucket/my-path/ex_table.parquet")
        ]

        Args:
            s3fs_conn_id (str): airflow connection of the type S3
            sql (str): a sql query that will return a value to be compared with the expected result
            register_s3_tables (list, optional): list of tables referenced in the sql statement. Default value is a empty list
            error_message (str, optional): the message of error to be logged. Defaults to "Error during quality test".
            expected_result (any, optional): a value to be compared with the sql query response. Defaults to 1.
        """

        # initializing inheritance
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        # defining operator properties
        self.sql = sql
        self.expected_result = expected_result
        self.error_message = error_message
        self.s3fs_conn_id = s3fs_conn_id
        self.register_s3_tables = register_s3_tables

    def execute(self, context):
        """
        This will be executed as the operator is activated.

        Args:
            context (dict): airflow context

        Raises:
            ValueError: Error raised when sql statement response is different of expected value.
        """

        self.log.info("Running data quality checks...")
        self.log.info("Prepared SQL: %s" % self.sql)

        # requesting a filesystem-like on top of s3
        s3fs = S3fsHook(conn_id=self.s3fs_conn_id)
        fs = s3fs.get_filesystem()

        # creating a memory database connection
        con = duckdb.connect(database=":memory:")

        # loading each table to memory
        for table_name, s3_path, *others in self.register_s3_tables:
            columns = others[0] if len(others) else None
            self.log.info(f"Prepared path for {table_name}: {s3_path}")

            paths = fs.glob(s3_path)
            tmp_path = paths if len(paths) > 1 else paths[0]

            con.register(
                table_name,
                pq.read_table(tmp_path, filesystem=fs, columns=columns),
            )

        record = con.execute(self.sql).fetchone()
        self.log.info("Data quality check returned the value %s." % record[0])

        # checking for expected values
        if record[0] != self.expected_result:
            raise ValueError(self.error_message)
