# fmt: off
from pyspark.sql import SparkSession
from pyspark.sql.functions import (expr, broadcast)
from pyspark.sql.types import (StructType, StructField, StringType, FloatType)

import argparse
# fmt: on

# defining arguments
parser = argparse.ArgumentParser(
    description="""
Stations Spark ETL >
This ETL extracts s3 ghcn data loaded from ncdc to s3,
makes a join with stations (with administrative areas).
The result table is saved back to S3.
"""
)

parser.add_argument(
    "--s3-ghcn-src-path",
    help="s3 path where the ghcn files were stored",
    type=str,
    required=True,
)
parser.add_argument(
    "--s3-ghcn-path",
    help="s3 path where the final table will be saved",
    type=str,
    required=True,
)
parser.add_argument(
    "--s3-stations-path",
    help="s3 path where the final table will be saved",
    type=str,
    required=True,
)


def extract_ghcn_data(spark, s3_ghcn_src_path):
    """
    This extracts ghcn data from s3 and return
    a dataframe.

    Args:
        spark (SparkSession): the spark session
        s3_ghcn_src_path (str): ghcn data src path

    Returns:
        DataFrame: src data converted to spark dataframe
    """

    # defining schema
    ghcn_schema = StructType(
        [
            StructField("station", StringType(), False),
            StructField("date", StringType(), False),
            StructField("element", StringType(), False),
            StructField("value", StringType(), False),
            StructField("measurement_flag", StringType(), True),
            StructField("quality_flag", StringType(), True),
            StructField("source_flag", StringType(), True),
            StructField("obs_time", StringType(), True),
        ]
    )

    return (
        spark.read.format("csv")
        .option("header", "false")
        .schema(ghcn_schema)
        .load(s3_ghcn_src_path)
    )


def load_to_s3(spark, df_ghcn_station, s3_ghcn_path):
    """
    This is responsible for storing data back to s3

    Args:
        spark (SparkSession): the spark session
        df_ghcn_station (DataFrame): dataframe of ghcn information
        s3_ghcn_path (str): dst path to store ghcn table
    """

    # gathering administrative columns
    adm_columns = [
        column for column in df_ghcn_station.columns if column.startswith("adm")
    ]

    # set dynamic mode to preserve previous month of times saved
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        df_ghcn_station.selectExpr(
            "station",
            "element",
            "measurement_flag",
            "quality_flag",
            "source_flag",
            "CAST (value AS INT) as value",
            "TO_TIMESTAMP(CONCAT(date, CASE WHEN obs_time IS NULL THEN '0000' ELSE obs_time END), 'yyyyMMddHHmm') as datetime",
            "distance as distance_from_station",
            *adm_columns,
        )
        .withColumn("year", expr("YEAR(datetime)"))
        .withColumn("month", expr("MONTH(datetime)"))
        .repartition("element", "year", "month")
        .write.partitionBy("element", "year", "month")
        .mode("overwrite")
        .format("parquet")
        .save(s3_ghcn_path)
    )


def main(s3_stations_path, s3_ghcn_path, s3_ghcn_src_path):
    """
    This creates a spark session and coordinate the ETL steps,
    then it closes the spark session

    Args:
        s3_stations_path (str): stations table path
        s3_ghcn_path (str): ghcn dst table path
        s3_ghcn_src_path (str): ghcn src file path
    """
    spark = SparkSession.builder.getOrCreate()

    df_ghcn = extract_ghcn_data(spark, s3_ghcn_src_path)

    df_stations = spark.read.format("parquet").load(s3_stations_path)

    df_ghcn_station = broadcast(df_stations).join(
        df_ghcn, on=expr("id = station"), how="inner"
    )

    load_to_s3(spark, df_ghcn_station, s3_ghcn_path)

    spark.stop()


if __name__ == "__main__":

    # parsing arguments and starting the script
    args = parser.parse_args()
    main(**args.__dict__)
