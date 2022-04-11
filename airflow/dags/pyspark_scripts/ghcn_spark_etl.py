# fmt: off
from pyspark.sql import SparkSession
from pyspark.sql.functions import (expr, broadcast)
from pyspark.sql.types import (StructType, StructField, StringType, FloatType)
# fmt: on

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


def extract_ghcn_data(spark):
    s3_source = spark.conf.get("spark.executorEnv.S3_GHCN_SRC_PATH")

    return (
        spark.read.format("csv")
        .option("header", "false")
        .schema(ghcn_schema)
        .load(s3_source)
    )


def load_to_s3(spark, df_ghcn_station):
    s3_ghcn_table = spark.conf.get("spark.executorEnv.S3_GHCN_PATH")
    adm_columns = [column for column in df_stations.columns if column.startswith("adm")]

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
        .save(s3_ghcn_table)
    )


def main():
    spark = SparkSession.builder.getOrCreate()

    df_ghcn = extract_ghcn_data(spark)

    s3_load_path = spark.conf.get("spark.executorEnv.S3_STATIONS_PATH")
    df_stations = spark.read.format("parquet").load(s3_load_path)

    df_ghcn_station = broadcast(df_stations).join(
        df_ghcn, on=expr("id = station"), how="inner"
    )

    load_to_s3(spark, df_ghcn_station)

    spark.stop()


if __name__ == "__main__":
    main()
