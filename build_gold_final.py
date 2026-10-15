from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Complete_Gold_Final") \
    .master("local[2]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

GOLD_DIR = "file:///data/softs/datnt_workspace/output/gold"
OUT_CSV_DIR = "file:///home/datnt/bigdata_project/output"

print("--- 1. BUILDING DIM_DOMAIN ---")
df_domain = spark.read.csv(f"{OUT_CSV_DIR}/dim_domain.csv", header=True, inferSchema=True)
df_domain.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_domain")
print(f"dim_domain done: {df_domain.count()} rows")

print("--- 2. BUILDING DIM_DATE ---")
df_date = spark.read.csv(f"{OUT_CSV_DIR}/dim_date.csv", header=True, inferSchema=True)
df_date.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_date")
print(f"dim_date done: {df_date.count()} rows")

print("--- 3. ENSURING DIM_TIME ---")
rows_time = [
    {"time_key": h, "hour_of_day": h,
     "time_slot": "Office Hours" if 8 <= h <= 17 else "Off Hours",
     "period_of_day": ("Morning" if 5 <= h <= 11
                       else "Afternoon" if 12 <= h <= 17
                       else "Evening" if 18 <= h <= 22
                       else "Night")}
    for h in range(24)
]
df_time = spark.createDataFrame(rows_time)
df_time.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_time")
print(f"dim_time done: {df_time.count()} rows")

print("--- 4. ENSURING DIM_PLATFORM ---")
df_plat = spark.createDataFrame([
    {"platform_key": 1, "platform_name": "Mobile", "tracking_id_type": "Advertising ID", "network_type": "4G/5G"},
    {"platform_key": 2, "platform_name": "PC", "tracking_id_type": "Browser Cookie", "network_type": "FTTH"}
])
df_plat.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_platform")
print(f"dim_platform done: {df_plat.count()} rows")

print("--- 5. ENSURING DIM_LOCATION ---")
df_loc = spark.read.csv(f"{OUT_CSV_DIR}/dim_location.csv", header=True, inferSchema=True)
df_loc.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_location")
print(f"dim_location done: {df_loc.count()} rows")

print("--- 6. BUILDING FACT_PAGEVIEW_HOURLY ---")
df_m_hourly = spark.read.csv(f"{OUT_CSV_DIR}/bai3_mobile_hourly_pattern.csv", header=True, inferSchema=True)
df_pc_hourly = spark.read.csv(f"{OUT_CSV_DIR}/bai3_pc_hourly_pattern.csv", header=True, inferSchema=True)
df_m_top = spark.read.csv(f"{OUT_CSV_DIR}/bai1_top10_mobile_overall.csv", header=True, inferSchema=True).join(df_domain, on="clean_domain").select("domain_key", "pageviews")
df_pc_top = spark.read.csv(f"{OUT_CSV_DIR}/bai1_top10_pc_overall.csv", header=True, inferSchema=True).join(df_domain, on="clean_domain").select("domain_key", "pageviews")

tot_m_views = df_m_hourly.agg(sum("total_views")).collect()[0][0] or 1
tot_pc_views = df_pc_hourly.agg(sum("total_views")).collect()[0][0] or 1

df_fact_m = df_m_hourly.withColumn("platform_key", lit(1)) \
    .withColumn("date_key", lit(20260902)) \
    .withColumn("time_key", col("hour_of_day").cast(IntegerType())) \
    .withColumn("location_key", lit(1)) \
    .crossJoin(df_m_top) \
    .withColumn("pageview_count", (col("pageviews") * (col("total_views").cast(DoubleType()) / lit(tot_m_views))).cast(LongType())) \
    .withColumn("unique_visitor_count", (col("avg_uv") * (col("pageviews").cast(DoubleType()) / lit(tot_m_views))).cast(LongType())) \
    .select("platform_key", "date_key", "time_key", "domain_key", "location_key", "pageview_count", "unique_visitor_count")

df_fact_pc = df_pc_hourly.withColumn("platform_key", lit(2)) \
    .withColumn("date_key", lit(20260902)) \
    .withColumn("time_key", col("hour_of_day").cast(IntegerType())) \
    .withColumn("location_key", lit(1)) \
    .crossJoin(df_pc_top) \
    .withColumn("pageview_count", (col("pageviews") * (col("total_views").cast(DoubleType()) / lit(tot_pc_views))).cast(LongType())) \
    .withColumn("unique_visitor_count", (col("avg_uv") * (col("pageviews").cast(DoubleType()) / lit(tot_pc_views))).cast(LongType())) \
    .select("platform_key", "date_key", "time_key", "domain_key", "location_key", "pageview_count", "unique_visitor_count")

fact_final = df_fact_m.unionByName(df_fact_pc)
fact_final.write.partitionBy("platform_key").mode("overwrite").parquet(f"{GOLD_DIR}/fact_pageview_hourly")
print(f"fact_pageview_hourly done: {fact_final.count()} rows")

fact_final.toPandas().to_csv(f"/home/datnt/bigdata_project/output/fact_pageview_hourly.csv", index=False)
print("fact_pageview_hourly.csv exported!")

print("=== ALL 6 GOLD TABLES BUILT SUCCESSFULLY ===")
spark.stop()
