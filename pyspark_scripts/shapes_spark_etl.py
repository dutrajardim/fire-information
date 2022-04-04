# fmt: off
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

import re
# fmt: on


def save_relations(spark, df_relations):
    shapes_relations_path = spark.conf.get(
        "spark.executorEnv.S3_OSM_SHAPES_RELATIONS_PATH"
    )

    (
        df_relations.repartition("adm")
        .write.partitionBy("adm")
        .mode("overwrite")
        .format("parquet")
        .save(shapes_relations_path)
    )


def save_shapes(spark, df_shapes):
    shapes_path = spark.conf.get("spark.executorEnv.S3_OSM_SHAPES_PATH")

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
        .save(shapes_path)
    )


def extract_relations(df_shapes):
    df_children = df_shapes.selectExpr(
        "admin_level",
        "osm_id",
        "name",
        "EXPLODE_OUTER(SPLIT(parents, ',')) AS parent_id",
    )

    # adm will be parent in the relation
    parent = df_shapes.selectExpr(
        "admin_level AS parent_level", "osm_id AS parent_id", "name AS parent_name"
    ).alias("parent")

    children = df_children.selectExpr(
        "admin_level AS adm", "osm_id AS id", "parent_id", "name"
    ).alias("children")

    # get children with their parents
    df_relations = parent.join(children, "parent_id", "right")

    # group by children and make set relations in rows to columns
    pt_relations = (
        df_relations.groupBy(["adm", "id", "name"])
        .pivot("parent_level")
        .agg(expr("FIRST(parent_id) AS adm"), expr("FIRST(parent_name) AS adm_name"))
    )

    pt_relations = pt_relations.drop("null_adm", "null_adm_name")

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


def main():
    spark = SparkSession.builder.getOrCreate()
    shapes_src_path = spark.conf.get("spark.executorEnv.S3_OSM_SHAPES_SRC_PATH")

    df_adm = spark.read.format("json").load(shapes_src_path)
    df_relations = extract_relations(df_adm)

    save_relations(spark, df_relations)
    save_shapes(spark, df_adm)
    spark.stop()


if __name__ == "__main__":
    main()
