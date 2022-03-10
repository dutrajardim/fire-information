# fmt: off
from pyspark.sql import SparkSession
from pyspark.sql.functions import (expr, broadcast)
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType, FloatType, TimestampType)

from sedona.utils.adapter import Adapter
from sedona.core.enums import (GridType, IndexType)
from sedona.core.spatialOperator import JoinQueryRaw
# fmt: on


schema = StructType(
    [
        StructField("geometry", StringType(), False),
        StructField("brightness", FloatType(), True),
        StructField("frp", FloatType(), True),
        StructField("scan", FloatType(), True),
        StructField("track", FloatType(), True),
        StructField("confidence", StringType(), True),
        StructField("type", IntegerType(), True),
        StructField("instrument", StringType(), True),
        StructField("datetime", TimestampType(), True),
        StructField("year", IntegerType(), True),
        StructField("adm0", StringType(), False),
        StructField("adm1", StringType(), False),
        StructField("adm2", StringType(), False),
        StructField("adm3", StringType(), True),
    ]
)


def extract_firms_data(spark):

    bucket = "s3a://dutrajardim-fi"
    years_str = "2018, 2019, 2020, 2021".replace(" ", "")
    s3_source = "%s/src/firms/suomi_viirs_c2/archive/{%s}.csv.gz" % (bucket, years_str)

    df_firms = spark.read.format("csv").option("header", "true").load(s3_source)

    sdf_firms = df_firms.selectExpr(
        "ST_GeomFromWKT(CONCAT('POINT(', longitude, ' ', latitude, ')')) as geometry",
        "brightness",
        "frp",
        "scan",
        "track",
        "confidence",
        "type",
        "instrument",
        "CONCAT(acq_date, ' ', regexp_replace(acq_time, '(.{2})(.{2})', '$1:$2')) as datetime",
        "SUBSTRING(acq_date, 1, 4) as year",
    )

    rdd_firms = Adapter.toSpatialRdd(sdf_firms, "geometry")
    rdd_firms.analyze()

    return rdd_firms


def extract_shapes_data(spark):

    bucket = "s3a://dutrajardim-fi"
    s3_adm2_src = "%s/tables/shapes/adm2.parquet" % bucket
    s3_adm3_src = "%s/tables/shapes/adm3.parquet" % bucket

    df_adm3 = spark.read.format("parquet").load(s3_adm2_src)
    df_adm2 = spark.read.format("parquet").load(s3_adm3_src)

    sdf_adm3 = df_adm3.selectExpr(
        "id as adm3",
        "ST_GeomFromWKT(geometry) as geometry_adm3",
        "CONCAT(CONCAT_WS('.', SLICE(SPLIT(id, '\\\\.'), 1, 3)), '_1') as adm2",
    )

    sdf_adm2 = df_adm2.selectExpr(
        "id as adm2", "ST_GeomFromWKT(geometry) as geometry_adm2"
    )

    sdf_adm = broadcast(sdf_adm2).join(sdf_adm3, on="adm2", how="left")
    sdf_adm = sdf_adm.selectExpr(
        "CASE WHEN geometry_adm3 IS NOT NULL THEN geometry_adm3 ELSE geometry_adm2 END as geometry",
        "ELEMENT_AT(SPLIT(adm2, '\\\\.'), 1) as adm0",
        "CONCAT(CONCAT_WS('.', SLICE(SPLIT(adm2, '\\\\.'), 1, 2)), '_1') as adm1",
        "adm2",
        "adm3",
    )

    rdd_adm = Adapter.toSpatialRdd(sdf_adm, "geometry")
    rdd_adm.analyze()

    return rdd_adm


def load_to_s3(sdf_firm_adm):

    bucket = "s3a://dutrajardim-fi"
    s3_firms_table = "%s/tables/firms.parquet" % bucket

    # set dynamic mode to preserve previous month of times saved
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    sdf_firm_adm = sdf_firm_adm.selectExpr(
        "ST_AsText(rightgeometry) AS geometry",
        "CAST(brightness as FLOAT)",
        "CAST(frp as FLOAT)",
        "CAST(scan as FLOAT)",
        "CAST(track as FLOAT)",
        "confidence",
        "CAST(type as INT)",
        "instrument",
        "TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm') as datetime",
        "CAST(year as INT)",
        "adm0",
        "adm1",
        "adm2",
        "adm3",
    )

    # fmt: off
    sdf_firm_adm.repartition("adm0", "adm1", "year") \
        .write \
        .partitionBy("adm0", "adm1", "year") \
        .option("schema", schema) \
        .mode("overwrite") \
        .format("parquet") \
        .save(s3_firms_table)
    # fmt: on


def spatial_join(rdd_firms, rdd_adm):
    rdd_adm.spatialPartitioning(GridType.KDBTREE)
    rdd_firms.spatialPartitioning(rdd_adm.getPartitioner())

    # second param is buildIndexOnSpatialPartitionedRDD - set to true as we will run a join query
    rdd_firms.buildIndex(IndexType.QUADTREE, True)

    # third param set using index to true while the fourth param set consider boundary intersection to true
    query_result = JoinQueryRaw.SpatialJoinQueryFlat(rdd_firms, rdd_adm, True, True)

    adm_columns = ["adm0", "adm1", "adm2", "adm3"]
    firms_columns = [
        "brightness",
        "frp",
        "scan",
        "track",
        "confidence",
        "type",
        "instrument",
        "datetime",
        "year",
    ]

    return Adapter.toDf(query_result, adm_columns, firms_columns, spark)


def main():
    spark = SparkSession.builder.appName("DJ - Fire Information").getOrCreate()

    rdd_firms = extract_firms_data(spark)
    rdd_adm = extract_shapes_data(spark)

    sdf_firm_adm = spatial_join(rdd_firms, rdd_adm)

    load_to_s3(sdf_firm_adm)

    spark.stop()
