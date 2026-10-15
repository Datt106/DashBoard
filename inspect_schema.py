from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Inspect").master("local[*]").getOrCreate()
df = spark.read.parquet("hdfs://10.5.94.214:19000/data_ec/ad-pageview-mobile/parquet/2026-09-02")
df.printSchema()
df.show(5, truncate=False)
