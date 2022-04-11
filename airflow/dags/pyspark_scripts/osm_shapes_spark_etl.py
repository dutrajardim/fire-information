# fmt: off
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

import re
import argparse
# fmt: on

# defining arguments
parser = argparse.ArgumentParser(
    description="""
Shapes Spark ETL >
This ETL extracts s3 shapes data, loaded from Open Street Map,
works on the hierarchical relations of the administrative areas,
and save both, relations and shapes, in independent tables on s3.
"""
)

parser.add_argument(
    "--s3-osm-shapes-path",
    help="s3 path where the shapes table will be saved",
    type=str,
    required=True,
)

parser.add_argument(
    "--s3-osm-relations-path",
    help="s3 path where the relations table will be saved",
    type=str,
    required=True,
)
parser.add_argument(
    "--s3-osm-shapes-src-path",
    help="s3 path where OSM shapes were stored",
    type=str,
    required=True,
)


def extract_relations(df_shapes):
    """
    This function is responsible for transform hierarchical relations
    of the shapes that are stored in the same column, in different
    columns to facilitate feature works grouping firms data and
    ghcn data by region.

    Args:
        df_shapes (DataFrame): dataframe with shapes information

    Returns:
        DataFrame: dataframe with relations information
    """

    # relations in src table are all stored in the same column
    # separated by comma, so first we create one row for each relation
    children = df_shapes.selectExpr(
        "admin_level as adm",
        "osm_id as id",
        "name",
        "EXPLODE_OUTER(SPLIT(parents, ',')) AS parent_id",  # exploding each parent in a new row
    )

    # to discovery parent's administrative level,
    # running a join with itself, as id is unique
    # for all shapes

    # taking id and level to be the table of parents
    parent = df_shapes.selectExpr(
        "admin_level AS parent_level", "osm_id AS parent_id", "name AS parent_name"
    ).alias("parent")

    # we need a join to correlate parent level to the parent_id in first table
    df_relations = parent.join(children, on="parent_id", how="right")

    # creating a pivot table to convert parent_id to columns
    pt_relations = (
        df_relations.groupBy(["adm", "id", "name"])
        .pivot("parent_level")
        .agg(expr("FIRST(parent_id) AS adm"), expr("FIRST(parent_name) AS adm_name"))
    )

    # as the root level there is no parent, removing generated null columns
    pt_relations = pt_relations.drop("null_adm", "null_adm_name")

    # renaming generated columns
    # ex. 1_adm and 1_adm_name to adm1 and adm1_name
    pattern = re.compile("^(([0-9]{,2})_adm(_name)?)$")
    columns_to_rename = [
        pattern.match(column).groups()
        for column in pt_relations.columns
        if pattern.match(column)
    ]

    for col, adm_id, is_name in columns_to_rename:
        pt_relations = pt_relations.withColumnRenamed(
            col, f"adm{adm_id}_name" if is_name else f"adm{adm_id}"
        )

    return pt_relations


def load_relations(spark, df_relations, s3_osm_relations_path):
    """
    This is responsible for sotoring relations data to s3

    Args:
        spark (SparkSession): the spark session
        df_relations (DataFrame): dataframe of relations
        s3_osm_relations_path (str): dst path to store the relations
    """
    (
        df_relations.repartition("adm")
        .write.partitionBy("adm")
        .mode("overwrite")
        .format("parquet")
        .save(s3_osm_relations_path)
    )


def load_shapes(spark, df_shapes, s3_osm_shapes_path):
    """
    This is responsible for sotoring shapes data to s3

    Args:
        spark (SparkSession): the spark session
        df_shapes (DataFrame): dataframe of shapes source
        s3_osm_shapes_path (str): dst path to store the shapes
    """

    (
        df_shapes.selectExpr(
            "SUBSTRING_INDEX(geom, ';', -1) AS geometry",
            "admin_level AS adm",
            "osm_id AS id",
        )
        .repartition("adm")
        .write.partitionBy("adm")
        .option("maxRecordsPerFile", 1000)
        .mode("overwrite")
        .format("parquet")
        .save(s3_osm_shapes_path)
    )


def main(s3_osm_shapes_path, s3_osm_relations_path, s3_osm_shapes_src_path):
    """
    This creates a spark session to coordinate the ETL pipeline and
    then it closes the spark session

    Args:
        s3_osm_shapes_path (str): shapes dst table path
        s3_osm_relations_path (str): relations dst table path
        s3_osm_shapes_src_path (str): shapes src file path
    """
    spark = SparkSession.builder.getOrCreate()

    # loading shapes from source path
    df_adm = spark.read.format("json").load(s3_osm_shapes_src_path)

    # extracting relations information
    df_relations = extract_relations(df_adm)

    # saving work to s3
    load_relations(spark, df_relations, s3_osm_relations_path)
    load_shapes(spark, df_adm, s3_osm_shapes_path)

    spark.stop()


if __name__ == "__main__":

    # parsing arguments and starting the script
    args = parser.parse_args()
    main(**args.__dict__)
