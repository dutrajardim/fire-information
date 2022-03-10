# fmt: off
from pyspark.sql import SparkSession
from pyspark.sql.functions import (expr, broadcast)
from pyspark.sql.types import (StructType, StructField, StringType)
# fmt: on

load_schema = StructType(
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


def extract_ghcn_data(spark):
    bucket = "s3a://dutrajardim-fi"
    s3_source = "%s/src/ncdc/ghcn/{2018,2019,2020,2021}.csv.gz" % bucket

    # fmt: off
    return spark.read \
        .format("csv") \
        .option("header", "false") \
        .schema(schema) \
        .load(s3_source)
    # fmt: on


def load_to_s3(df_ghcn_station):
    bucket = "s3a://dutrajardim-fi"
    s3_ghcn_table = "%s/tables/ghcn.parquet" % bucket

    # set dynamic mode to preserve previous month of times saved
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # fmt: off
    df_ghcn_station \
        .selectExpr(
            "station",
            "CAST (value aS INT) as value",
            "element",
            "measurement_flag",
            "quality_flag",
            "source_flag",
            "TO_TIMESTAMP(CONCAT(date, CASE WHEN obs_time IS NULL THEN '0000' ELSE obs_time END), 'yyyyMMddHHmm') as datetime",
            "adm0",
            "adm1",
            "adm2",
            "adm3"
        ) \
        .withColumn("year", expr("YEAR(datetime)")) \
        .repartition("element", "adm0", "adm1", "year") \
        .write \
        .partitionBy("element", "adm0", "adm1", "year") \
        .mode("overwrite") \
        .format("parquet") \
        .save(s3_ghcn_table)
    # fmt: on


def main():
    spark = SparkSession.builder.appName("DJ - GHCN Information").getOrCreate()

    df_ghcn = extract_ghcn_data(spark)

    df_stations = spark.read.format("parquet").load(
        "s3a://dutrajardim-fi/tables/stations.parquet"
    )
    df_ghcn_station = broadcast(df_stations).join(
        df_ghcn, on=expr("id = station"), how="inner"
    )

    load_to_s3(df_ghcn_station)

    spark.stop()
