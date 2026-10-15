import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, size, to_timestamp, date_format, hour, sum as _sum, approx_count_distinct, lower, trim, regexp_replace
from pyspark.sql.types import IntegerType, LongType

def main():
    spark = SparkSession.builder \
        .appName("CompleteGoldBuilder") \
        .master("local[*]") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    GOLD_BASE = "file:///data/softs/datnt_workspace/output/gold"
    LOCAL_OUTPUT = "/home/datnt/bigdata_project/output"

    print("=== 1. DIM TIME ===")
    rows = [{"time_key": h, "hour_of_day": h, 
             "time_slot": "Office Hours" if 8 <= h <= 17 else "Off Hours",
             "period_of_day": "Morning" if 5 <= h <= 11 else ("Afternoon" if 12 <= h <= 17 else ("Evening" if 18 <= h <= 22 else "Night"))}
            for h in range(24)]
    dim_time = spark.createDataFrame(rows)
    dim_time.write.mode("overwrite").parquet(GOLD_BASE + "/dim_time")
    print("dim_time OK, count:", dim_time.count())

    print("=== 2. DIM PLATFORM ===")
    dim_platform = spark.createDataFrame([
        {"platform_key": 1, "platform_name": "Mobile", "tracking_id_type": "Advertising ID", "network_type": "4G/5G"},
        {"platform_key": 2, "platform_name": "PC", "tracking_id_type": "Browser Cookie", "network_type": "FTTH"}
    ])
    dim_platform.write.mode("overwrite").parquet(GOLD_BASE + "/dim_platform")
    print("dim_platform OK, count:", dim_platform.count())

    print("=== 3. DIM LOCATION ===")
    if os.path.exists(LOCAL_OUTPUT + "/dim_location.csv"):
        dim_location = spark.read.option("header", "true").csv("file://" + LOCAL_OUTPUT + "/dim_location.csv")
    else:
        dim_location = spark.read.parquet(GOLD_BASE + "/dim_location")
    dim_location = dim_location.withColumn("location_key", col("location_key").cast(IntegerType()))
    dim_location.write.mode("overwrite").parquet(GOLD_BASE + "/dim_location")
    print("dim_location OK, count:", dim_location.count())

    print("=== 4. DIM DOMAIN ===")
    dim_domain = spark.read.option("header", "true").csv("file://" + LOCAL_OUTPUT + "/dim_domain.csv")
    dim_domain = dim_domain.withColumn("domain_key", col("domain_key").cast(IntegerType()))
    dim_domain.write.mode("overwrite").parquet(GOLD_BASE + "/dim_domain")
    print("dim_domain OK, count:", dim_domain.count())

    print("=== 5. DIM DATE ===")
    dim_date = spark.read.option("header", "true").csv("file://" + LOCAL_OUTPUT + "/dim_date.csv")
    dim_date = dim_date.withColumn("date_key", col("date_key").cast(IntegerType()))
    dim_date.write.mode("overwrite").parquet(GOLD_BASE + "/dim_date")
    print("dim_date OK, count:", dim_date.count())

    print("=== 6. FACT PAGEVIEW HOURLY ===")
    dom_broadcast = spark.read.parquet(GOLD_BASE + "/dim_domain")

    def clean_domain_expr(c):
        d = lower(trim(c))
        d = regexp_replace(d, r"^https?://", "")
        d = regexp_replace(d, r"^m[www]\.", "")
        d = regexp_replace(d, r"^www\.", "")
        d = regexp_replace(d, r":\d+", "")
        d = regexp_replace(d, r"/.*$", "")
        return d

    import shutil
    fact_path = GOLD_BASE.replace("file://", "") + "/fact_pageview_hourly"
    if os.path.exists(fact_path):
        shutil.rmtree(fact_path)

    dates = ["2026-09-02", "2026-09-03", "2026-09-04", "2026-09-05", "2026-09-06", "2026-09-07", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11"]
    
    for d_str in dates:
        m_path = f"hdfs://10.5.94.214:19000/data_ec/ad-pageview-mobile/parquet/{d_str}"
        try:
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
                    approx_count_distinct("user_id").alias("unique_visitor_count")
                )
            df_m.write.mode("append").partitionBy("platform_key").parquet(GOLD_BASE + "/fact_pageview_hourly")
            print(f"Processed & appended mobile {d_str}")
        except Exception as e:
            print(f"Error mobile {d_str}:", e)

        pc_path = f"hdfs://10.5.94.214:19000/data_ec/ad-pageview-pc/parquet/{d_str}"
        try:
            df_pc = spark.read.parquet(pc_path)
            df_pc = df_pc.withColumn("split_val", split(col("value"), "\t")) \
                .filter(size(col("split_val")) > 13) \
                .select(
                    col("split_val").getItem(0).alias("raw_time"),
                    col("split_val").getItem(8).alias("raw_domain"),
                    col("split_val").getItem(13).alias("user_id")
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
                    approx_count_distinct("user_id").alias("unique_visitor_count")
                )
            df_pc.write.mode("append").partitionBy("platform_key").parquet(GOLD_BASE + "/fact_pageview_hourly")
            print(f"Processed & appended PC {d_str}")
        except Exception as e:
            print(f"Error PC {d_str}:", e)

    print("Consolidating fact table across partitions...")
    all_fact = spark.read.parquet(GOLD_BASE + "/fact_pageview_hourly")
    fact_final = all_fact.groupBy("platform_key", "date_key", "time_key", "domain_key", "location_key") \
        .agg(
            _sum("pageview_count").alias("pageview_count"),
            _sum("unique_visitor_count").alias("unique_visitor_count")
        ) \
        .select(
            col("platform_key").cast(IntegerType()),
            col("date_key").cast(IntegerType()),
            col("time_key").cast(IntegerType()),
            col("domain_key").cast(IntegerType()),
            col("location_key").cast(IntegerType()),
            col("pageview_count").cast(LongType()),
            col("unique_visitor_count").cast(LongType())
        )
    fact_final.write.partitionBy("platform_key").mode("overwrite").parquet(GOLD_BASE + "/fact_pageview_hourly")
    print("fact_pageview_hourly OK, count:", fact_final.count())

    print("\n=== VERIFICATION ===")
    for t in ["dim_time", "dim_platform", "dim_location", "dim_domain", "dim_date", "fact_pageview_hourly"]:
        try:
            df = spark.read.parquet(GOLD_BASE + "/" + t)
            print(f"Table {t}: {df.count()} rows")
        except Exception as e:
            print(f"Table {t}: Error reading - {e}")

if __name__ == "__main__":
    main()
