# fmt: off
from pyspark.sql import SparkSession
from pyspark.sql.functions import (expr, broadcast)
from pyspark.sql.types import (StructType, StructField, StringType, FloatType)

from sedona.utils.adapter import Adapter
from sedona.core.enums import (GridType, IndexType)
from sedona.core.spatialOperator import JoinQueryRaw
from sedona.core.SpatialRDD import CircleRDD
# fmt: on

schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("geometry", StringType(), False),
        StructField("name", StringType(), True),
        StructField("elevation", FloatType(), True),
        StructField("distance", FloatType(), False),
        StructField("amd0", StringType(), False),
        StructField("amd1", StringType(), False),
        StructField("amd2", StringType(), False),
        StructField("amd3", StringType(), True),
    ]
)

sourceCrsCode = "epsg:4326"  # WGS84, the most common degree-based CRS
targetCrsCode = "epsg:3857"  # The most common meter-based CRS


def extract_stations_data(spark):
    # ID            1-11   Character
    # LATITUDE     13-20   Real
    # LONGITUDE    22-30   Real
    # ELEVATION    32-37   Real
    # STATE        39-40   Character
    # NAME         42-71   Character
    # GSN FLAG     73-75   Character
    # HCN/CRN FLAG 77-79   Character
    # WMO ID       81-85   Character

    bucket = "s3a://dutrajardim-fi"
    s3_source = "%s/src/ncdc/stations.txt.gz" % bucket
    df_stations = spark.read.format("text").load(s3_source)

    sdf_stations = df_stations.selectExpr(
        "substring(value, 1, 11) as id",
        "CAST(trim(substring(value, 13, 8)) AS Decimal(24,20)) as latitude",
        # "trim(substring(value, 22, 9)) as longitude",
        "trim(substring(value, 32, 6)) as elevation",
        # "substring('value', 39, 2) as state",
        "trim(substring(value, 42, 30)) as name",
        # "substring(value, 73, 3) as gsn_flag",
        # "substring(value, 77, 3) as hcn_crn_flag",
        # "substring(value, 81, 5) as wmo_id",
        "ST_GeomFromWKT(CONCAT('POINT (', trim(substring(value, 13, 8)), ' ', trim(substring(value, 22, 9)), ')') ) as geometry",  # flipping coordenates
        # "ST_Point(CAST(trim(substring(value, 13, 8)) AS Decimal(24,20)), CAST(trim(substring(value, 22, 9)) AS Decimal(24,20))) as geometry",
    )

    # remove points near poles for mercartor projection transformation
    # fmt: off
    sdf_stations = sdf_stations \
        .filter("latitude > -80 and latitude < 84") \
        .drop("latitude")
    # fmt: on

    rdd_stations = Adapter.toSpatialRdd(sdf_stations, "geometry")
    rdd_stations.CRSTransform(
        sourceEpsgCRSCode=sourceCrsCode, targetEpsgCRSCode=targetCrsCode
    )

    rdd_stations.analyze()
    return rdd_stations


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
        "ST_FlipCoordinates(CASE WHEN geometry_adm3 IS NOT NULL THEN geometry_adm3 ELSE geometry_adm2 END) as geometry",
        "ELEMENT_AT(SPLIT(adm2, '\\\\.'), 1) as adm0",
        "CONCAT(CONCAT_WS('.', SLICE(SPLIT(adm2, '\\\\.'), 1, 2)), '_1') as adm1",
        "adm2",
        "adm3",
    )

    rdd_adm = Adapter.toSpatialRdd(sdf_adm, "geometry")
    rdd_adm.CRSTransform(
        sourceEpsgCRSCode=sourceCrsCode, targetEpsgCRSCode=targetCrsCode
    )
    rdd_adm.analyze()

    return rdd_adm


def spatial_join(rdd_stations, rdd_adm, spark):

    max_distance = 50000.0

    rdd_circle = CircleRDD(rdd_stations, max_distance)
    rdd_circle.analyze()

    rdd_circle.spatialPartitioning(GridType.KDBTREE)
    rdd_adm.spatialPartitioning(rdd_circle.getPartitioner())

    considerBoundaryIntersection = True  # Do not only return gemeotries fully covered by each query window in rdd_circle
    usingIndex = False

    query_result = JoinQueryRaw.DistanceJoinQueryFlat(
        rdd_adm, rdd_circle, usingIndex, considerBoundaryIntersection
    )

    adm_cols = ["adm0", "adm1", "adm2", "adm3"]
    station_cols = ["id", "elevation", "name"]
    sdf_stations_adm = Adapter.toDf(query_result, station_cols, adm_cols, spark)

    # fmt: off
    return sdf_stations_adm \
        .withColumn("distance", expr("ST_Distance(leftgeometry, rightgeometry)")) \
        .withColumn("station_rank", expr("RANK() OVER (PARTITION BY (adm2, adm3) ORDER BY distance)")) \
        .where("station_rank < 4")
    # fmt: on


def load_to_s3(sdf_stations_adm):

    bucket = "s3a://dutrajardim-fi"
    s3_firms_table = "%s/tables/stations.parquet" % bucket

    # set dynamic mode to preserve previous month of times saved
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    sdf_stations_adm = sdf_stations_adm.selectExpr(
        "id",
        "ST_AsText(leftgeometry) as geometry",
        "name",
        "CAST(elevation AS FLOAT) as elevation",
        "CAST(distance AS FLOAT) as distance",
        "adm0",
        "adm1",
        "adm2",
        "adm3",
    )

    # fmt: off
    sdf_stations_adm.repartition("adm0") \
        .write \
        .partitionBy("adm0") \
        .option("schema", schema) \
        .mode("overwrite") \
        .format("parquet") \
        .save("s3a://dutrajardim-fi/tables/stations.parquet")
    # fmt: on


def main():
    spark = SparkSession.builder.appName("DJ - Station Information").getOrCreate()

    rdd_stations = extract_stations_data(spark)
    rdd_adm = extract_shapes_data(spark)

    sdf_stations_adm = spatial_join(rdd_stations, rdd_adm, spark)

    load_to_s3(sdf_stations_adm)

    spark.stop()
