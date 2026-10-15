import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, size, to_timestamp, date_format, hour, sum as _sum, count, lower, trim, regexp_replace
from pyspark.sql.types import IntegerType

def main():
    spark = SparkSession.builder \
        .appName("BuildGoldAll") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "30") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    GOLD_BASE = "file:///data/softs/datnt_workspace/output/gold"
    LOCAL_OUTPUT = "/home/datnt/bigdata_project/output"

    print("=== DIMENSIONS ===")
    rows = [{"time_key": h, "hour_of_day": h, 
             "time_slot": "Office Hours" if 8 <= h <= 17 else "Off Hours",
             "period_of_day": "Morning" if 5 <= h <= 11 else ("Afternoon" if 12 <= h <= 17 else ("Evening" if 18 <= h <= 22 else "Night"))}
            for h in range(24)]
    spark.createDataFrame(rows).write.mode("overwrite").parquet(GOLD_BASE + "/dim_time")

    spark.createDataFrame([
        {"platform_key": 1, "platform_name": "Mobile", "tracking_id_type": "Advertising ID", "network_type": "4G/5G"},
        {"platform_key": 2, "platform_name": "PC", "tracking_id_type": "Browser Cookie", "network_type": "FTTH"}
    ]).write.mode("overwrite").parquet(GOLD_BASE + "/dim_platform")

    spark.read.option("header", "true").csv("file://" + LOCAL_OUTPUT + "/dim_location.csv") \
        .withColumn("location_key", col("location_key").cast(IntegerType())) \
        .write.mode("overwrite").parquet(GOLD_BASE + "/dim_location")

    spark.read.option("header", "true").csv("file://" + LOCAL_OUTPUT + "/dim_domain.csv") \
        .withColumn("domain_key", col("domain_key").cast(IntegerType())) \
        .write.mode("overwrite").parquet(GOLD_BASE + "/dim_domain")

    spark.read.option("header", "true").csv("file://" + LOCAL_OUTPUT + "/dim_date.csv") \
        .withColumn("date_key", col("date_key").cast(IntegerType())) \
        .write.mode("overwrite").parquet(GOLD_BASE + "/dim_date")

    print("Dimensions created successfully.")

    print("=== FACT PAGEVIEW HOURLY ===")
    dom_broadcast = spark.read.parquet(GOLD_BASE + "/dim_domain")

    def clean_domain_expr(c):
        d = lower(trim(c))
        d = regexp_replace(d, r"^https?://", "")
        d = regexp_replace(d, r"^m[www]\.", "")
        d = regexp_replace(d, r"^www\.", "")
        d = regexp_replace(d, r":\d+", "")
        d = regexp_replace(d, r"/.*$", "")
        return d

    dates = [
        "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-05", 
        "2026-09-06", "2026-09-07", "2026-09-08", "2026-09-09", 
        "2026-09-10", "2026-09-11"
    ]

    import shutil
    fact_path = GOLD_BASE.replace("file://", "") + "/fact_pageview_hourly"
    if os.path.exists(fact_path):
        shutil.rmtree(fact_path)

    first = True
    for d_str in dates:
        try:
            print(f"Processing mobile date {d_str}...")
            m_path = f"hdfs://10.5.94.214:19000/data_ec/ad-pageview-mobile/parquet/{d_str}"
            df_m = spark.read.parquet(m_path)
            df_m = df_m.withColumn("split_val", split(col("value"), "\t")) \
                .filter(size(col("split_val")) > 11) \
                .select(
                    col("split_val").getItem(0).alias("raw_time"),
                    col("split_val").getItem(8).alias("raw_domain"),
                    col("split_val").getItem(11).alias("user_id")
                ).filter(col("raw_time").isNotNull() & col("raw_domain").isNotNull() & col("user_id").isNotNull() & (col("user_id") != "")) \
                .withColumn("clean_domain", clean_domain_expr(col("raw_domain"))) \
                .filter(col("clean_domain") != "") \
                .withColumn("access_time", to_timestamp(col("raw_time"), "yyyy-MM-dd HH:mm:ss")) \
                .filter(col("access_time").isNotNull()) \
                .withColumn("date_key", date_format(col("access_time"), "yyyyMMdd").cast(IntegerType())) \
                .withColumn("time_key", hour(col("access_time"))) \
                .withColumn("platform_key", lit(1)) \
                .withColumn("location_key", lit(1)) \
                .join(dom_broadcast.select("clean_domain", "domain_key"), on="clean_domain", how="inner") \
                .groupBy("platform_key", "date_key", "time_key", "domain_key", "location_key") \
                .agg(
                    _sum(lit(1)).alias("pageview_count"),
                    count("user_id").alias("unique_visitor_count")
                )
            
            if first:
                df_m.write.mode("overwrite").partitionBy("platform_key").parquet(fact_path)
                first = False
            else:
                df_m.write.mode("append").partitionBy("platform_key").parquet(fact_path)
            print(f"Mobile date {d_str} success.")
        except Exception as e:
            print(f"Error processing mobile {d_str}: {e}")

    print("Mobile facts processed.")

    for d_str in dates:
        try:
            print(f"Processing PC date {d_str}...")
            pc_path = f"hdfs://10.5.94.214:19000/data_ec/ad-pageview-pc/parquet/{d_str}"
            df_pc = spark.read.parquet(pc_path)
            df_pc = df_pc.withColumn("split_val", split(col("value"), "\t")) \
                .filter(size(col("split_val")) > 11) \
                .select(
                    col("split_val").getItem(0).alias("raw_time"),
                    col("split_val").getItem(8).alias("raw_domain"),
                    col("split_val").getItem(11).alias("user_id")
                ).filter(col("raw_time").isNotNull() & col("raw_domain").isNotNull() & col("user_id").isNotNull() & (col("user_id") != "")) \
                .withColumn("clean_domain", clean_domain_expr(col("raw_domain"))) \
                .filter(col("clean_domain") != "") \
                .withColumn("access_time", to_timestamp(col("raw_time"), "yyyy-MM-dd HH:mm:ss")) \
                .filter(col("access_time").isNotNull()) \
                .withColumn("date_key", date_format(col("access_time"), "yyyyMMdd").cast(IntegerType())) \
                .withColumn("time_key", hour(col("access_time"))) \
                .withColumn("platform_key", lit(2)) \
                .withColumn("location_key", lit(1)) \
                .join(dom_broadcast.select("clean_domain", "domain_key"), on="clean_domain", how="inner") \
                .groupBy("platform_key", "date_key", "time_key", "domain_key", "location_key") \
                .agg(
                    _sum(lit(1)).alias("pageview_count"),
                    count("user_id").alias("unique_visitor_count")
                )
            
            df_pc.write.mode("append").partitionBy("platform_key").parquet(fact_path)
            print(f"PC date {d_str} success.")
        except Exception as e:
            print(f"PC path {pc_path} not found or error: {e}")

    print("Verification:")
    for t in ["dim_time", "dim_platform", "dim_location", "dim_domain", "dim_date", "fact_pageview_hourly"]:
        try:
            df = spark.read.parquet(GOLD_BASE + "/" + t)
            print(f"Table {t}: {df.count()} rows")
        except Exception as e:
            print(f"Table {t} error: {e}")

if __name__ == "__main__":
    main()
