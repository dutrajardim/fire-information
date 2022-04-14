# fmt: off
from pyspark.sql import SparkSession
from pyspark.sql.functions import (expr, broadcast)
from pyspark.conf import SparkConf
from pyspark.sql.types import (StructType, StructField, StringType, FloatType)

from sedona.register import SedonaRegistrator
from sedona.utils import (KryoSerializer, SedonaKryoRegistrator)
from sedona.utils.adapter import Adapter
from sedona.core.enums import (GridType, IndexType)
from sedona.core.spatialOperator import JoinQueryRaw
from sedona.core.SpatialRDD import CircleRDD

import argparse
# fmt: on


# defining arguments
parser = argparse.ArgumentParser(
    description="""
Stations Spark ETL >
This ETL extracts s3 stations data loaded from ncdc to s3,
makes a spatial join with geo shapes of administrative areas and
a join with hierarchical relations of the administrative area.
The result table is saved back to S3.
"""
)

parser.add_argument(
    "--s3-stations-src-path",
    help="s3 path where the FWF (fixed width format) stations file were stored",
    type=str,
    required=True,
)

parser.add_argument(
    "--s3-stations-path",
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


def extract_stations_data(spark, s3_stations_src_path):
    """
    This extracts fwf stations data from s3 and return
    a spatial rdd.

    Args:
        spark (SparkSession): the spark session
        s3_stations_src_path (str): fwf stations data src path

    Returns:
        SpatialRDD: src data converted to spatial rdd
    """

    # > Fixed Width Format table
    # ID            1-11   Character
    # LATITUDE     13-20   Real
    # LONGITUDE    22-30   Real
    # ELEVATION    32-37   Real
    # STATE        39-40   Character
    # NAME         42-71   Character
    # GSN FLAG     73-75   Character
    # HCN/CRN FLAG 77-79   Character
    # WMO ID       81-85   Character

    df_stations = spark.read.format("text").load(s3_stations_src_path)

    sdf_stations = df_stations.selectExpr(
        "substring(value, 1, 11) as id",
        "CAST(trim(substring(value, 13, 8)) AS Decimal(24,20)) as latitude",
        "trim(substring(value, 32, 6)) as elevation",
        "trim(substring(value, 42, 30)) as name",
        # flipping coordinates because converting CRS function is flipping it (lat, log instead of log, lat)
        "ST_GeomFromWKT(CONCAT('POINT (', trim(substring(value, 13, 8)), ' ', trim(substring(value, 22, 9)), ')') ) as geometry",
    )

    # remove points near poles for mercartor projection transformation
    sdf_stations = sdf_stations.filter("latitude > -80 and latitude < 84").drop(
        "latitude"
    )

    return Adapter.toSpatialRdd(sdf_stations, "geometry")


def extract_shapes_data(spark, s3_shapes_path):
    """
    This extracts shapes table data from s3 and
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
        .selectExpr(
            "ST_FlipCoordinates(ST_GeomFromWKT(geometry)) AS geometry",  # flipping coordinates because converting CRS is flipping it
            f"id AS shape_id",
        )
    )

    return Adapter.toSpatialRdd(df_shapes, "geometry")


def spatial_join(spark, rdd_stations, rdd_shapes, max_distance=50000.0):
    """
    This is responsible for joining stations and adm areas whithin
    the given max distance

    Args:
        spark (SparkSession): the spark session
        rdd_stations (SpatialRDD): stations data
        rdd_shapes (SpatialRDD): shapes data
        max_distance (float, optional): shapes and stations within this distance will be joined. Defaults to 50000.0.

    Returns:
        DataFrame: stations with adm area id
    """

    # transforming CRS as spatial join does not take in account the used unit
    rdd_stations.CRSTransform(
        "epsg:4326", "epsg:3857"
    )  # transform to meters-based CRS (it is fliping coordinates)
    rdd_stations.analyze()

    rdd_shapes.CRSTransform(
        "epsg:4326", "epsg:3857"
    )  # transform to meters-based CRS (it is fliping coordinates)
    rdd_shapes.analyze()

    # defining shapes to join based in stations location point
    rdd_circle = CircleRDD(rdd_stations, max_distance)
    rdd_circle.analyze()

    # creating partitions
    rdd_circle.spatialPartitioning(GridType.KDBTREE)
    rdd_shapes.spatialPartitioning(rdd_circle.getPartitioner())

    # defining join params
    considerBoundaryIntersection = True  # Do not only return geometries fully covered by each query window in rdd_circle
    usingIndex = False

    query_result = JoinQueryRaw.DistanceJoinQueryFlat(
        rdd_shapes, rdd_circle, usingIndex, considerBoundaryIntersection
    )

    # converting to dataframe (left columns are related to stations)
    sdf_stations_adm = Adapter.toDf(
        query_result, ["id", "elevation", "name"], ["shape_id"], spark
    )

    # calc distance between stations and adm, then select
    # up to 2 or 3 nearest adm area, and transform CRS back to degrees
    return (
        sdf_stations_adm.withColumn(
            "distance", expr("ST_Distance(leftgeometry, rightgeometry)")
        )
        .withColumn(
            "station_rank",
            expr(f"RANK() OVER (PARTITION BY (shape_id) ORDER BY distance)"),
        )
        .where(
            "station_rank < 4"
        )  # 1 to distance equal 0 if station inside amd area or first nearest, 2 and 3 for take more 2 nearest
        .withColumn(
            "geometry",
            expr(
                "ST_FlipCoordinates(ST_Transform(leftgeometry, 'epsg:3857', 'epsg:4326'))"
            ),  # transform to degree-based CRS (it is fliping coordinates, so I flip to undo it)
        )
    )


# sharing alias between functions
stations_str = "stations"
relations_str = "relations"


def relations_join(spark, sdf_stations, s3_relations_path):
    """
    This is responsible for joining the stations dataframe
    with the administrative spatial areas relations dataframe

    Args:
        spark (SparkSession): the spark session
        sdf_stations (DataFrame): stations dataframe
        s3_relations_path (str): relations table path

    Returns:
        DataFrame: stations with adm areas
    """

    df_relations = spark.read.format("parquet").load(s3_relations_path)
    return sdf_stations.alias(stations_str).join(
        broadcast(df_relations).alias(relations_str),
        on=sdf_stations.shape_id == df_relations.id,
        how="inner",
    )


def load_to_s3(spark, sdf_stations_rel, s3_stations_path):
    """
    This is responsible for storing data back to s3

    Args:
        spark (SparkSession): the spark session
        sdf_stations_rel (DataFrame): dataframe returned from relations_join
        s3_stations_path (str): dst path to store stations table
    """

    adm_columns = [
        column for column in sdf_stations_rel.columns if column.startswith("adm")
    ]

    # set static mode to rewrite all previous data saved
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    (
        sdf_stations_rel.selectExpr(
            f"{stations_str}.id",
            "ST_AsText(geometry) as geometry",
            f"{stations_str}.name",
            "CAST(elevation AS FLOAT) as elevation",
            "CAST(distance AS FLOAT) as distance",
            f"{relations_str}.id AS adm",
            f"{relations_str}.name AS adm_name",
            *adm_columns,
        )
        .write.mode("overwrite")
        .format("parquet")
        .save(s3_stations_path)
    )


def main(s3_stations_src_path, s3_stations_path, s3_relations_path, s3_shapes_path):
    """
    This creates a spark session to coordinate the ETL pipeline and
    then it closes the spark session

    Args:
        s3_stations_src_path (str): stations src file path
        s3_stations_path (str): stations dst table path
        s3_relations_path (str): relations table path
        s3_shapes_path (str): shapes table path
    """

    spark = SparkSession.builder.getOrCreate()
    # registering spatial sql functions
    SedonaRegistrator.registerAll(spark)

    # extracting the data
    rdd_stations = extract_stations_data(spark, s3_stations_src_path)
    rdd_shapes = extract_shapes_data(spark, s3_shapes_path)

    # joining tables
    sdf_stations = spatial_join(spark, rdd_stations, rdd_shapes)
    sdf_stations_rel = relations_join(spark, sdf_stations, s3_relations_path)

    # saving work
    load_to_s3(spark, sdf_stations_rel, s3_stations_path)

    spark.stop()


if __name__ == "__main__":

    # parsing arguments and starting the script
    args = parser.parse_args()
    main(**args.__dict__)
