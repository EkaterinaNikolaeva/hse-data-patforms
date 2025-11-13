from onetl.connection import Hive, SparkHDFS
from onetl.file import FileDFReader
from onetl.file.format import Parquet
from onetl.db import DBWriter
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("yarn")
    .appName("testSpark")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("spark.hive.metastore.uris", "thrift://team-1-nn:9083")
    .enableHiveSupport()
    .getOrCreate()
)

hdfs = SparkHDFS(host="team-1-nn", port=9000, spark=spark, cluster="smthng")
reader = FileDFReader(connection=hdfs, format=Parquet(), source_path="/input/")
df = reader.run(["history.parquet"])
df = df.groupBy("date").agg({"rain_mm": "max"}).withColumn("year", df.date.substr(0, 4))
hive = Hive(spark=spark, cluster="smthng")
writer=DBWriter(connection=hive, target="test.history",options=Hive.WriteOptions(partitionBy=["year"]))
writer.run(df)
spark.stop()
