from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
GOLD_BASE = "file:///data/softs/datnt_workspace/output/gold"
for t in ["dim_time", "dim_platform", "dim_location", "dim_domain", "dim_date", "fact_pageview_hourly"]:
    try:
        df = spark.read.parquet(GOLD_BASE + "/" + t)
        print(f"Table {t}: {df.count()} rows")
        df.show(3, truncate=False)
    except Exception as e:
        print(f"Table {t} error: {e}")
