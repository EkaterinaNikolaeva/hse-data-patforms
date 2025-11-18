from prefect import flow, task
from prefect.tasks import NO_CACHE
from pyspark.sql import SparkSession, functions
from onetl.connection import Hive, SparkHDFS
from onetl.file import FileDFReader
from onetl.file.format import Parquet
from onetl.db import DBWriter
import os


@task(name="init_spark", cache_policy=NO_CACHE)
def init_spark(app_name="prefect-flow"):
    os.environ["HADOOP_CONF_DIR"] = os.environ.get(
        "HADOOP_CONF_DIR", "/home/hadoop/hadoop-3.4.0/etc/hadoop"
    )

    spark = (
        SparkSession.builder.master("yarn")
        .appName(app_name)
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.hive.metastore.uris", "thrift://team-1-nn:9083")
        .enableHiveSupport()
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    return spark


@task(name="stop_spark", cache_policy=NO_CACHE)
def stop_spark(spark):
    spark.stop()


@task(name="extract", cache_policy=NO_CACHE)
def extract(
    spark,
    hdfs_host="team-1-nn",
    hdfs_port=9000,
    source_path="/input/",
    file_pattern="history.parquet",
):
    hdfs = SparkHDFS(host=hdfs_host, port=hdfs_port, spark=spark, cluster="smthng")
    reader = FileDFReader(connection=hdfs, format=Parquet(), source_path=source_path)
    df = reader.run([file_pattern])
    return df


@task(name="transform", cache_policy=NO_CACHE)
def transform(df):
    df = df.withColumn(
        "date_clean",
        functions.to_date("date"),
    ).filter(functions.col("date_clean").isNotNull())
    df = df.withColumn(
        "year",
        functions.year("date_clean"),
    ).withColumn(
        "month",
        functions.month("date_clean"),
    )
    df = df.withColumn("is_rain", (functions.col("rain_mm") > 0).cast("int"))

    df_agg = df.groupBy("date_clean", "year", "month").agg(
        functions.max("rain_mm").alias("rain_max"),
        functions.min("rain_mm").alias("rain_min"),
        functions.avg("rain_mm").alias("rain_avg"),
        functions.sum("is_rain").alias("rain_events"),
    )

    return df_agg


@task(name="load", cache_policy=NO_CACHE)
def load(spark, df, table_name="test.history", partition_by=["year"]):
    spark.sql("CREATE DATABASE IF NOT EXISTS test")
    hive = Hive(spark=spark, cluster="smthng")
    writer = DBWriter(
        connection=hive,
        target=table_name,
        options=Hive.WriteOptions(
            partitionBy=partition_by,
        ),
    )
    writer.run(df)


@flow(name="prefect_flow")
def process_data(
    hdfs_source_path="/input/",
    file_pattern="history.parquet",
    table_name="test.history",
    partition_by=["year"],
    hdfs_host="team-1-nn",
    hdfs_port=9000,
):

    spark = None
    try:
        spark = init_spark(app_name="prefect-flow")

        df = extract(
            spark,
            hdfs_host=hdfs_host,
            hdfs_port=hdfs_port,
            source_path=hdfs_source_path,
            file_pattern=file_pattern,
        )

        df_transformed = transform(df)

        load(spark, df_transformed, table_name=table_name, partition_by=partition_by)

    except Exception as e:
        raise
    finally:
        if spark is not None:
            stop_spark(spark)


if __name__ == "__main__":
    process_data()
