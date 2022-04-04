# fmt: off
from pyspark.sql import SparkSession
from pyspark.sql.functions import (expr, broadcast)

from sedona.utils.adapter import Adapter
from sedona.core.enums import (GridType, IndexType)
from sedona.core.spatialOperator import JoinQueryRaw
from sedona.register import SedonaRegistrator
# fmt: on


def extract_firms_data(spark):

    s3_source = spark.conf.get("spark.executorEnv.S3_FIRMS_SRC_PATH")
    df_firms = spark.read.format("csv").option("header", "true").load(s3_source)

    sdf_firms = df_firms.selectExpr(
        "ST_GeomFromWKT(CONCAT('POINT(', longitude, ' ', latitude, ')')) as geometry",
        "bright_ti4",
        "bright_ti5",
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

    adm_level = spark.conf.get("spark.executorEnv.ADM_LEVEL")
    shapes_path = spark.conf.get("spark.executorEnv.S3_SHAPES_PATH")

    df_shapes = spark.read.format("parquet").load(shapes_path)

    sdf_adm = df_shapes.selectExpr(
        "ST_GeomFromWKT(geometry) as geometry", f"id AS adm{adm_level}"
    )

    rdd_adm = Adapter.toSpatialRdd(sdf_adm, "geometry")
    rdd_adm.analyze()

    return rdd_adm


def load_to_s3(spark, sdf_firm_adm):

    s3_firms_table = spark.conf.get("spark.executorEnv.S3_FIRMS_PATH")
    adm_level = spark.conf.get("spark.executorEnv.ADM_LEVEL")

    # set dynamic mode to preserve previous month of times saved
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        sdf_firm_adm.selectExpr(
            "ST_AsText(rightgeometry) AS geometry",
            "CAST(bright_ti4 as FLOAT)",
            "CAST(bright_ti5 as FLOAT)",
            "CAST(frp as FLOAT)",
            "CAST(scan as FLOAT)",
            "CAST(track as FLOAT)",
            "confidence",
            "CAST(type as INT)",
            "instrument",
            "TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm') as datetime",
            "CAST(year as INT)",
            "MONTH(TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm')) AS month",
            f"adm{adm_level}",
        )
        .repartition("year", "month")
        .write.partitionBy("year", "month")
        .mode("overwrite")
        .format("parquet")
        .save(s3_firms_table)
    )


def spatial_join(spark, rdd_firms, rdd_adm):
    rdd_adm.spatialPartitioning(GridType.KDBTREE)
    rdd_firms.spatialPartitioning(rdd_adm.getPartitioner())

    # second param is buildIndexOnSpatialPartitionedRDD - set to true as we will run a join query
    rdd_firms.buildIndex(IndexType.QUADTREE, True)

    # third param set using index to true while the fourth param set consider boundary intersection to true
    query_result = JoinQueryRaw.SpatialJoinQueryFlat(rdd_firms, rdd_adm, True, True)

    return Adapter.toDf(query_result, rdd_adm.fieldNames, rdd_firms.fieldNames, spark)


def main():
    spark = SparkSession.builder.getOrCreate()
    SedonaRegistrator.registerAll(spark)

    rdd_firms = extract_firms_data(spark)
    rdd_adm = extract_shapes_data(spark)

    sdf_firm_adm = spatial_join(spark, rdd_firms, rdd_adm)

    load_to_s3(spark, sdf_firm_adm)

    spark.stop()


if __name__ == "__main__":
    main()
