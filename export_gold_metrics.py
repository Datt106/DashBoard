from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, desc, row_number

spark = SparkSession.builder \
    .appName("Export-Kimball-Gold-Metrics") \
    .master("local[4]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

GOLD_BASE = "hdfs://10.5.94.214:19000/data_ec/gold"

print(">>> Đang đọc các bảng Star Schema đã hoàn thành từ HDFS Gold...")
fact_final = spark.read.parquet(f"{GOLD_BASE}/fact_pageview_hourly").cache()
dim_domain = spark.read.parquet(f"{GOLD_BASE}/dim_domain").cache()
dim_date = spark.read.parquet(f"{GOLD_BASE}/dim_date").cache()
dim_location = spark.read.parquet(f"{GOLD_BASE}/dim_location").cache()

for p_key, p_name in [(1, "mobile"), (2, "pc")]:
    print(f"\n[+] Đang trích xuất dữ liệu cho nền tảng: {p_name.upper()}...")
    df_p = fact_final.filter(col("platform_key") == p_key) \
                     .join(dim_domain, "domain_key") \
                     .join(dim_date, "date_key") \
                     .join(dim_location, "location_key") \
                     .cache()

    # Bài 1: Overall Top 10
    top10_overall = df_p.groupBy("clean_domain") \
                        .agg({"pageview_count": "sum"}) \
                        .withColumnRenamed("sum(pageview_count)", "pageviews") \
                        .orderBy(desc("pageviews")).limit(10)
    top10_overall.toPandas().to_csv(f"output/bai1_top10_{p_name}_overall.csv", index=False)
    print(f"  -> Xong bai1_top10_{p_name}_overall.csv")

    # Bài 1: Daily Top 10
    w_daily = Window.partitionBy("full_date").orderBy(desc("pageviews"))
    daily_top10 = df_p.groupBy("full_date", "clean_domain") \
                      .agg({"pageview_count": "sum"}) \
                      .withColumnRenamed("sum(pageview_count)", "pageviews") \
                      .withColumn("rank", row_number().over(w_daily)) \
                      .filter(col("rank") <= 10).orderBy("full_date", "rank")
    daily_top10.toPandas().to_csv(f"output/bai1_top10_{p_name}_daily.csv", index=False)
    print(f"  -> Xong bai1_top10_{p_name}_daily.csv")

    # Bài 2: Phân bổ địa lý website Top 1
    top1_dom = top10_overall.first()["clean_domain"]
    df_top1 = df_p.filter(col("clean_domain") == top1_dom)
    tot = df_top1.agg({"pageview_count": "sum"}).collect()[0][0] or 1

    df_top1.groupBy("country").agg({"pageview_count": "sum"}) \
           .withColumnRenamed("sum(pageview_count)", "pageviews") \
           .withColumn("percentage", (col("pageviews") / tot) * 100) \
           .orderBy(desc("pageviews")).toPandas().to_csv(f"output/bai2_geo_top1_{p_name}_country.csv", index=False)
    print(f"  -> Xong bai2_geo_top1_{p_name}_country.csv")

    df_top1.filter(col("country") == "Vietnam").groupBy("city").agg({"pageview_count": "sum"}) \
           .withColumnRenamed("sum(pageview_count)", "pageviews") \
           .withColumn("percentage", (col("pageviews") / tot) * 100) \
           .orderBy(desc("pageviews")).toPandas().to_csv(f"output/bai2_geo_top1_{p_name}_city.csv", index=False)
    print(f"  -> Xong bai2_geo_top1_{p_name}_city.csv")

    # Bài 3: 24h Hourly Pattern
    df_p.groupBy("time_key") \
        .agg({"unique_visitor_count": "sum", "pageview_count": "sum"}) \
        .withColumnRenamed("time_key", "hour_of_day") \
        .withColumnRenamed("sum(unique_visitor_count)", "avg_uv") \
        .withColumnRenamed("sum(pageview_count)", "total_views") \
        .orderBy("hour_of_day").toPandas().to_csv(f"output/bai3_{p_name}_hourly_pattern.csv", index=False)
    print(f"  -> Xong bai3_{p_name}_hourly_pattern.csv")

print("\n===== HOÀN TẤT THÀNH CÔNG STAR SCHEMA VÀ TẦNG GOLD (KIMBALL) =====")
spark.stop()
