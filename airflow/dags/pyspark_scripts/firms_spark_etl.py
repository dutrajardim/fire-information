# fmt: off
from pyspark.sql import SparkSession
from pyspark.sql.functions import (expr, broadcast)
from pyspark.sql.types import (StructType, StructField, StringType)

from sedona.utils.adapter import Adapter
from sedona.core.enums import (GridType, IndexType)
from sedona.core.spatialOperator import JoinQuery
from sedona.register import SedonaRegistrator

import argparse
# fmt: on

# defining arguments
parser = argparse.ArgumentParser(
    description="""
Stations Spark ETL >
This ETL extracts s3 firms data loaded from nasa to s3,
makes a spatial join with geo shapes of administrative areas and
a join with hierarchical relations of the administrative area.
The result table is saved back to S3.
"""
)

parser.add_argument(
    "--s3-firms-src-path",
    help="s3 path where the firms files were stored",
    type=str,
    required=True,
)
parser.add_argument(
    "--s3-firms-path",
    help="s3 path where the final table will be saved",
    type=str,
    required=True,
)
parser.add_argument(
    "--s3-relations-path",
    help="s3 path where hierarchical relations of administrative shapes were stored",
    type=str,
    required=True,
)
parser.add_argument(
    "--s3-shapes-path",
    help="s3 path where the administrative shapes were stored",
    type=str,
    required=True,
)


def extract_firms_data(spark, s3_firms_src_path):
    """
    This extracts firms data from s3 and return
    a spatial rdd

    Args:
        spark (SparkSession): the spark session
        s3_firms_src_path (str): firms data src path

    Returns:
        SpatialRDD: src data converted to spatial rdd
    """
    df_firms = spark.read.format("csv").option("header", "true").load(s3_firms_src_path)

    sdf_firms = df_firms.selectExpr(
        "ST_GeomFromWKT(CONCAT('POINT(', longitude, ' ', latitude, ')')) as geometry",
        "bright_ti4",
        "bright_ti5",
        "frp",
        "scan",
        "track",
        "confidence",
        "CONCAT(acq_date, ' ', regexp_replace(acq_time, '(.{2}):?(.{2})', '$1:$2')) as datetime",
        "SUBSTRING(acq_date, 1, 4) as year",
    )

    return Adapter.toSpatialRdd(sdf_firms, "geometry")


def extract_shapes_data(spark, s3_shapes_path):
    """
    This stracts shapes table data from s3 and
    returns a spatial rdd

    Args:
        spark (SparkSession): the spark session
        s3_shapes_path (str):

    Returns:
        SpatialRDD: converted shapes table (with geometry field)
    """

    df_shapes = (
        spark.read.format("parquet")
        .load(s3_shapes_path)
        .selectExpr("ST_GeomFromWKT(geometry) AS geometry", f"id AS shape_id")
    )

    return Adapter.toSpatialRdd(df_shapes, "geometry")


def spatial_join(spark, rdd_firms, rdd_shapes):
    """
    This is responsible for joining firms and adm areas.

    Args:
        spark (SparkSession): the spark session
        rdd_firms (SpatialRDD): firms data
        rdd_shapes (SpatialRDD): shapes data

    Returns:
        DataFrame: firms with adm area
    """

    rdd_shapes.analyze()
    rdd_firms.analyze()

    # creating partitions
    rdd_shapes.spatialPartitioning(GridType.KDBTREE)
    rdd_firms.spatialPartitioning(rdd_shapes.getPartitioner())

    # set to true as we will run a join query
    build_index_on_spatial_partitioned = True

    # creating index
    rdd_firms.buildIndex(IndexType.QUADTREE, build_index_on_spatial_partitioned)

    # defining join params
    consider_boundary_intersection = True  # Do not only return geometries fully covered by each query window in rdd_circle
    using_index = False

    query_result = JoinQuery.SpatialJoinQueryFlat(
        rdd_firms, rdd_shapes, consider_boundary_intersection, using_index
    )

    # converting to dataframe
    sdf_firms_adm = query_result.map(
        # 0 is shapes and 1 is firms
        lambda pair: [
            pair[1].geom.wkt,  # getting right geometry
            *pair[1].userData.split("\t"),
            *pair[0].userData.split("\t"),
        ]
    ).toDF(
        StructType(
            [
                StructField("geometry", StringType(), False),
                *[
                    StructField(name, StringType(), False)
                    for name in rdd_firms.fieldNames
                ],
                StructField("shape_id", StringType(), False),
            ]
        )
    )

    return sdf_firms_adm


def relations_join(spark, sdf_firms, s3_relations_path):
    """
    This is responsible for joining the firms dataframe
    with the administrative spatial areas dataframe

    Args:
        spark (SparkSession): the spark session
        sdf_firms (DataFrame): firms dataframe
        s3_relations_path (str): relations table path

    Returns:
        DataFrame: firms with adm areas
    """

    df_relations = spark.read.format("parquet").load(s3_relations_path)
    return sdf_firms.join(
        broadcast(df_relations), on=sdf_firms.shape_id == df_relations.id, how="inner"
    )


def load_to_s3(spark, sdf_firms_rel, s3_firms_path):
    """
    This is responsible for storing data back to s3

    Args:
        spark (SparkSession): the spark session
        sdf_firms_rel (DataFrame): dataframe of firms information
        s3_firms_path (str): dst path to store firms table
    """

    # gathering administrative columns
    adm_columns = [
        column for column in sdf_firms_rel.columns if column.startswith("adm")
    ]

    # set dynamic mode to preserve previous month of times saved
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        sdf_firms_rel.selectExpr(
            "geometry",
            "CAST(bright_ti4 as FLOAT)",
            "CAST(bright_ti5 as FLOAT)",
            "CAST(frp as FLOAT)",
            "CAST(scan as FLOAT)",
            "CAST(track as FLOAT)",
            "confidence",
            "TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm') as datetime",
            "CAST(year as INT)",
            "MONTH(TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm')) AS month",
            "id AS adm",
            "name AS adm_name",
            *adm_columns,
        )
        .repartition("year", "month")
        .write.partitionBy("year", "month")
        .mode("overwrite")
        .format("parquet")
        .save(s3_firms_path)
    )


def main(s3_firms_path, s3_relations_path, s3_shapes_path, s3_firms_src_path):
    """
    This creates a spark session and coordinate the ETL steps,
    then it closes the spark session

    Args:
        s3_firms_path (str): firms dst table path
        s3_relations_path (str): relations table path
        s3_shapes_path (str): shapes table path
        s3_firms_src_path (str): firms src file path
    """

    spark = SparkSession.builder.getOrCreate()
    # registering spatial sql functions
    SedonaRegistrator.registerAll(spark)

    # extracting the data
    rdd_firms = extract_firms_data(spark, s3_firms_src_path)
    rdd_shapes = extract_shapes_data(spark, s3_shapes_path)

    # joining tables
    sdf_firms = spatial_join(spark, rdd_firms, rdd_shapes)
    sdf_firms_rel = relations_join(spark, sdf_firms, s3_relations_path)

    # saving work
    load_to_s3(spark, sdf_firms_rel, s3_firms_path)

    spark.stop()


if __name__ == "__main__":

    # parsing arguments and starting the script
    args = parser.parse_args()
    main(**args.__dict__)
