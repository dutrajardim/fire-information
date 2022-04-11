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

    return Adapter.toSpatialRdd(sdf_firms, "geometry")


def extract_shapes_data(spark):
    shapes_path = spark.conf.get("spark.executorEnv.S3_SHAPES_PATH")

    df_shapes = (
        spark.read.format("parquet")
        .load(shapes_path)
        .selectExpr("ST_GeomFromWKT(geometry) as geometry", f"id AS shape_id")
    )

    return Adapter.toSpatialRdd(df_shapes, "geometry")


def load_to_s3(spark, sdf_firms_rel):

    s3_firms_table = spark.conf.get("spark.executorEnv.S3_FIRMS_PATH")
    adm_columns = [
        column for column in sdf_firms_rel.columns if column.startswith("adm")
    ]

    # set dynamic mode to preserve previous month of times saved
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        sdf_firms_rel.selectExpr(
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
            "relations.id AS adm",
            "relations.name AS adm_name",
            *adm_columns,
        )
        .repartition("year", "month")
        .write.partitionBy("year", "month")
        .mode("overwrite")
        .format("parquet")
        .save(s3_firms_table)
    )


def spatial_join(spark, rdd_firms, rdd_shapes):

    rdd_shapes.analyze()
    rdd_firms.analyze()

    rdd_shapes.spatialPartitioning(GridType.KDBTREE)
    rdd_firms.spatialPartitioning(rdd_shapes.getPartitioner())

    # second param is buildIndexOnSpatialPartitionedRDD - set to true as we will run a join query
    rdd_firms.buildIndex(IndexType.QUADTREE, True)

    # third param set using index to true while the fourth param set consider boundary intersection to true
    query_result = JoinQueryRaw.SpatialJoinQueryFlat(rdd_firms, rdd_shapes, True, True)

    return Adapter.toDf(
        query_result, rdd_shapes.fieldNames, rdd_firms.fieldNames, spark
    )


def relations_join(spark, sdf_stations):
    relations_path = spark.conf.get("spark.executorEnv.S3_RELATIONS_PATH")
    df_relations = spark.read.format("parquet").load(relations_path)
    return sdf_stations.alias("stations").join(
        broadcast(df_relations).alias("relations"),
        on=expr("relations.id = stations.shape_id"),
        how="inner",
    )


def main():
    spark = SparkSession.builder.getOrCreate()
    SedonaRegistrator.registerAll(spark)

    rdd_firms = extract_firms_data(spark)
    rdd_shapes = extract_shapes_data(spark)

    sdf_firms = spatial_join(spark, rdd_firms, rdd_shapes)
    sdf_firms_rel = relations_join(spark, sdf_firms)

    load_to_s3(spark, sdf_firms_rel)

    spark.stop()


if __name__ == "__main__":
    main()
