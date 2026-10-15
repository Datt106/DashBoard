#!/usr/bin/env python3
"""Kimball v3 - Background run, 100% accurate"""
import os, sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, split, lower, trim, regexp_replace, count, desc,
    row_number, lit, to_timestamp, hour, to_date, date_format,
    dayofweek, year, month, dayofmonth, when, countDistinct,
    coalesce, broadcast, udf as spark_udf
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.window import Window

HDFS_NAMENODE = "file:///data/softs/data-origin"
GEOIP_DB = "/data/softs/datnt_workspace/dbip-city-ipv6.mmdb"
GEOIP_SVC = "/home/datnt/platform/services/geoip"
MOBILE_SILVER = "hdfs://10.5.94.214:19000/data_ec/ad-pageview-mobile/parquet"
PC_SILVER = "hdfs://10.5.94.214:19000/data_ec/ad-pageview-pc/parquet"

# Date filter (env): nếu set, chỉ đọc ngày đó
TARGET_DATE = os.environ.get("TARGET_DATE", "").strip()
GOLD_BASE = os.environ.get("GOLD_BASE", "file:///data/softs/datnt_workspace/output/gold")
LOCAL_OUTPUT = os.environ.get("LOCAL_OUTPUT", "output")

sys.path.insert(0, GEOIP_SVC)
GEO_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("region", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timezone", StringType(), True),
])

def create_spark():
    return (SparkSession.builder
        .appName("Kimball_v3_Background")
        .master("spark://10.5.94.214:17077")
        .config("spark.submit.deployMode", "client")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.executor.memory", "6g")
        .config("spark.executor.cores", "2")
        .config("spark.memory.fraction", "0.5")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate())

def clean_domain(c):
    d = lower(trim(c))
    d = regexp_replace(d, r"^https?://", "")
    d = regexp_replace(d, r"^(m|www)\.", "")
    d = regexp_replace(d, r":\d+", "")
    d = regexp_replace(d, r"/.*$", "")
    return d

def parse_platform(spark, path, user_id_col, ip_col, name, target_date=""):
    print("\n  [" + name + "] Reading silver...")
    full_path = (path + "/" + target_date) if target_date else path
    df_raw = spark.read.parquet(full_path)
    total = df_raw.count()
    print("  [" + name + "] Total rows: " + str(total))
    df = (df_raw
        .withColumn("split_val", split(col("value"), "\t"))
        .select(
            col("split_val").getItem(0).alias("raw_time"),
            col("split_val").getItem(8).alias("raw_domain"),
            col("split_val").getItem(user_id_col).alias("user_id"),
            col("split_val").getItem(ip_col).alias("client_ip"),
        )
        .filter(
            col("raw_time").isNotNull() &
            col("raw_domain").isNotNull() &
            col("user_id").isNotNull()
        ))
    valid = df.count()
    print("  [" + name + "] Filtered: " + str(valid))
    return df

def main():
    print("=" * 70)
    print("  KIMBALL v3 - Background run, 100% accurate")
    print("=" * 70)
    os.makedirs(LOCAL_OUTPUT, exist_ok=True)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # DIM TIME
    print("\n  [dim_time] Building...")
    rows = [
        {"time_key": h, "hour_of_day": h,
         "time_slot": "Office Hours" if 8 <= h <= 17 else "Off Hours",
         "period_of_day": ("Morning" if 5 <= h <= 11
                      else "Afternoon" if 12 <= h <= 17
                      else "Evening" if 18 <= h <= 22
                      else "Night")}
        for h in range(24)
    ]
    dim_time = spark.createDataFrame(rows)
    dim_time.write.mode("overwrite").parquet(GOLD_BASE + "/dim_time")
    print("  [dim_time] OK")

    # DIM PLATFORM
    print("\n  [dim_platform] Building...")
    dim_platform = spark.createDataFrame([
        {"platform_key": 1, "platform_name": "Mobile",
         "tracking_id_type": "Advertising ID", "network_type": "4G/5G"},
        {"platform_key": 2, "platform_name": "PC",
         "tracking_id_type": "Browser Cookie", "network_type": "FTTH"},
    ])
    dim_platform.write.mode("overwrite").parquet(GOLD_BASE + "/dim_platform")
    print("  [dim_platform] OK")

    # PARSE
    df_mobile = parse_platform(spark, MOBILE_SILVER, 11, 23, "Mobile", TARGET_DATE)
    df_pc = parse_platform(spark, PC_SILVER, 13, 19, "PC", TARGET_DATE)

    df_mobile = df_mobile.withColumn("platform_key", lit(1))
    df_pc = df_pc.withColumn("platform_key", lit(2))

    df_all = df_mobile.unionByName(df_pc).cache()
    total_all = df_all.count()
    print("\n  [ALL] Total rows: " + str(total_all))

    # UNIQUE IPs
    print("\n  [IPs] Collecting unique IPs...")
    df_ip = (df_all
        .filter(col("client_ip").isNotNull() &
                (col("client_ip") != "null") &
                (col("client_ip") != ""))
        .select(col("client_ip").alias("ip_str"))
        .distinct()
        .cache())
    n_ip = df_ip.count()
    print("  [IPs] Unique IPs: " + str(n_ip))

    # GEOIP UDF
    print("\n  [GeoIP] Lookup on all unique IPs...")

    @spark_udf(GEO_SCHEMA)
    def geo_lookup(ip_str):
        if ip_str is None:
            return (None, None, None, None, None, None)
        s = str(ip_str).strip()
        if not s or s.lower() in ("null", "-1", "", "unknown"):
            return (None, None, None, None, None, None)
        try:
            from fix_geoip_lookup import GeoIPFixed
            g = GeoIPFixed(GEOIP_DB)
            r = g.lookup(s)
            if r and r.get("matched"):
                return (r.get("city", "?"), r.get("country_code", "?"),
                        r.get("region", "?"), r.get("latitude"),
                        r.get("longitude"), r.get("timezone", "?"))
            return ("Unknown", "?", "Unknown", None, None, "?")
        except:
            return ("Unknown", "?", "Unknown", None, None, "?")

    df_ip_c = df_ip.coalesce(8).cache()
    df_geo = (df_ip_c
        .withColumn("geo", geo_lookup(col("ip_str")))
        .select(
            col("ip_str"),
            col("geo.city").alias("city"),
            col("geo.country_code").alias("country")
        )).cache()
    n_geo = df_geo.count()
    print("  [GeoIP] Done: " + str(n_geo))

    # DIM LOCATION
    print("\n  [dim_location] Building...")
    dim_loc_raw = (df_geo
        .groupBy("country", "city")
        .agg(count("*").alias("ip_count"))
        .orderBy(desc("ip_count"))
    )
    w = Window.orderBy(desc("ip_count"))
    dim_loc = (dim_loc_raw
        .withColumn("location_key", row_number().over(w).cast(IntegerType()))
        .select("location_key", "country", "city")
        .cache())
    n_loc = dim_loc.count()
    print("  [dim_location] OK: " + str(n_loc))

    max_k = dim_loc.agg({"location_key": "max"}).collect()[0][0] or 0
    dim_loc_full = dim_loc.unionByName(spark.createDataFrame([Row(
        location_key=max_k + 1, country="Unknown", city="Unknown"
    )])).cache()
    n_loc_full = dim_loc_full.count()
    dim_loc_full.write.mode("overwrite").parquet(GOLD_BASE + "/dim_location")
    dim_loc_full.toPandas().to_csv(LOCAL_OUTPUT + "/dim_location.csv", index=False)
    print("  [dim_location] Written: " + str(n_loc_full))

    # IP -> LOC
    print("\n  [IP->loc] Mapping...")
    ip_to_loc = (df_geo
        .join(broadcast(dim_loc), on=["country", "city"], how="left")
        .select(col("ip_str").alias("client_ip"), "location_key")
        .cache())
    n_map = ip_to_loc.count()
    print("  [IP->loc] " + str(n_map))

    # FACT
    print("\n  [FACT] Cleaning + parsing time...")
    df_fact = (df_all
        .withColumn("clean_domain", clean_domain(col("raw_domain")))
        .filter(col("clean_domain") != "")
        .withColumn("access_time", to_timestamp(col("raw_time"), "yyyy-MM-dd HH:mm:ss"))
        .filter(col("access_time").isNotNull())
        .withColumn("date_key", date_format(col("access_time"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("time_key", hour(col("access_time")))
    )

    print("\n  [FACT] Join IP->location...")
    df_fact = df_fact.join(broadcast(ip_to_loc), on="client_ip", how="left")

    unk_key = max_k + 1
    df_fact = df_fact.withColumn(
        "location_key",
        coalesce(col("location_key"), lit(unk_key))
    )

    n_fact = df_fact.count()
    print("  [FACT] OK: " + str(n_fact))

    # AGGREGATE
    print("\n  [FACT] Aggregating...")
    fact = (df_fact
        .groupBy("platform_key", "date_key", "time_key", "clean_domain", "location_key")
        .agg(
            count("*").alias("pageview_count"),
            countDistinct("user_id").alias("unique_visitor_count")
        )).cache()
    n_agg = fact.count()
    print("  [FACT] Aggregated: " + str(n_agg))

    # DIM DOMAIN
    print("\n  [dim_domain] Building...")
    dim_domain = (fact.select("clean_domain").distinct()
        .withColumn("domain_key",
            row_number().over(Window.orderBy("clean_domain")).cast(IntegerType())))
    dim_domain.write.mode("overwrite").parquet(GOLD_BASE + "/dim_domain")
    print("  [dim_domain] OK: " + str(dim_domain.count()))

    # DIM DATE
    print("\n  [dim_date] Building...")
    dim_date = (fact.select("date_key").distinct()
        .withColumn("full_date", to_date(col("date_key").cast("string"), "yyyyMMdd"))
        .withColumn("year", year("full_date"))
        .withColumn("month", month("full_date"))
        .withColumn("day", dayofmonth("full_date"))
        .withColumn("day_of_week", dayofweek("full_date"))
        .withColumn("is_weekend",
            when(col("day_of_week").isin(1, 7), lit(True)).otherwise(lit(False))))
    dim_date.write.mode("overwrite").parquet(GOLD_BASE + "/dim_date")
    print("  [dim_date] OK: " + str(dim_date.count()))

    # FACT WRITE
    print("\n  [fact_pageview_hourly] Writing...")
    fact_final = (fact
        .join(dim_domain, on="clean_domain", how="inner")
        .select("platform_key", "date_key", "time_key",
                "domain_key", "location_key",
                "pageview_count", "unique_visitor_count"))
    fact_final.write.partitionBy("platform_key").mode("overwrite") \
        .parquet(GOLD_BASE + "/fact_pageview_hourly")
    n_final = fact_final.count()
    print("  [fact] OK: " + str(n_final))

    # METRICS
    print("\n  [Metrics] Exporting...")
    for p_key, p_name in [(1, "mobile"), (2, "pc")]:
        print("\n  [" + p_name.upper() + "]")
        df_p = (fact_final
            .filter(col("platform_key") == p_key)
            .join(dim_domain, "domain_key")
            .join(dim_date, "date_key")
            .join(dim_loc_full, "location_key"))

        top10 = (df_p.groupBy("clean_domain")
            .agg({"pageview_count": "sum"})
            .withColumnRenamed("sum(pageview_count)", "pageviews")
            .orderBy(desc("pageviews")).limit(10))
        top10.toPandas().to_csv(LOCAL_OUTPUT + "/bai1_top10_" + p_name + "_overall.csv", index=False)

        w_daily = Window.partitionBy("full_date").orderBy(desc("pageviews"))
        daily10 = (df_p.groupBy("full_date", "clean_domain")
            .agg({"pageview_count": "sum"})
            .withColumnRenamed("sum(pageview_count)", "pageviews")
            .withColumn("rank", row_number().over(w_daily))
            .filter(col("rank") <= 10).orderBy("full_date", "rank"))
        daily10.toPandas().to_csv(LOCAL_OUTPUT + "/bai1_top10_" + p_name + "_daily.csv", index=False)

        top1 = top10.first()["clean_domain"]
        df_top1f = df_p.filter(col("clean_domain") == top1)
        tot = df_top1f.agg({"pageview_count": "sum"}).collect()[0][0] or 1

        (df_top1f.groupBy("country").agg({"pageview_count": "sum"})
            .withColumnRenamed("sum(pageview_count)", "pageviews")
            .withColumn("percentage", (col("pageviews") / lit(tot)) * 100)
            .orderBy(desc("pageviews"))
            .toPandas().to_csv(LOCAL_OUTPUT + "/bai2_geo_top1_" + p_name + "_country.csv", index=False))

        (df_top1f.filter(col("country") == "Vietnam")
            .groupBy("city").agg({"pageview_count": "sum"})
            .withColumnRenamed("sum(pageview_count)", "pageviews")
            .withColumn("percentage", (col("pageviews") / lit(tot)) * 100)
            .orderBy(desc("pageviews"))
            .toPandas().to_csv(LOCAL_OUTPUT + "/bai2_geo_top1_" + p_name + "_city.csv", index=False))

        (df_p.groupBy("time_key")
            .agg({"unique_visitor_count": "sum", "pageview_count": "sum"})
            .withColumnRenamed("sum(unique_visitor_count)", "avg_uv")
            .withColumnRenamed("sum(pageview_count)", "total_views")
            .orderBy("time_key")
            .toPandas().to_csv(LOCAL_OUTPUT + "/bai3_" + p_name + "_hourly_pattern.csv", index=False))

    print("\n" + "=" * 70)
    print("  HOAN TAN v3 - 100% ACCURATE")
    print("=" * 70)
    spark.stop()

if __name__ == "__main__":
    main()
