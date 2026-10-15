# Kimball Data Warehouse — Ad Pageview Analytics

> Pipeline Spark xây dựng kho dữ liệu theo mô hình **Kimball (star schema)** từ log pageview quảng cáo trên 2 nền tảng **Mobile** và **PC**, phục vụ phân tích top domain theo ngày, phân bố địa lý theo IP, và pattern truy cập theo giờ.

---

## 1. Mục tiêu dự án

Biến log pageview thô (dạng text tab-separated, lưu trên HDFS dưới định dạng Parquet) thành **kho dữ liệu dạng star schema** chuẩn Kimball, gồm:

- 1 bảng fact: `fact_pageview_hourly`
- 5 bảng dim: `dim_time`, `dim_date`, `dim_platform`, `dim_domain`, `dim_location`
- 4 bài toán phân tích mẫu (top domain, geo, hourly pattern)

Mục tiêu cuối: cung cấp dữ liệu sạch, chuẩn hoá, join được đa chiều, cho phép BI tool (hoặc analyst) truy vấn nhanh.

---

## 2. Tech stack

| Thành phần | Công nghệ |
|---|---|
| Compute | Apache **Spark 3.x** (PySpark) |
| Storage raw | **HDFS** (parquet) |
| Storage gold | **Local filesystem** (parquet + CSV) |
| GeoIP | **DB-IP** (mmdb file, lookup offline) |
| Orchestration | Bash script (`run_10days.sh`) |
| Output format | Parquet (gold) + CSV (analyst-friendly) |

---

## 3. Cấu trúc dữ liệu

### 3.1. Input (raw)

Trên HDFS, mỗi ngày 1 folder:

```
hdfs://10.5.94.214:19000/data_ec/ad-pageview-mobile/parquet/<YYYY-MM-DD>/
hdfs://10.5.94.214:19000/data_ec/ad-pageview-pc/parquet/<YYYY-MM-DD>/
```

Mỗi row là 1 bản ghi pageview dạng `value` (string) — các trường phân cách bằng `\t`. Số lượng trường khác nhau giữa 2 nền tảng:

| Platform | Số trường | Cấu trúc đặc biệt |
|---|---|---|
| **Mobile** | 28 | Một số trường là base64 / JSON |
| **PC** | 29 | Thêm 1 trường ở cuối (thường rỗng) |

### 3.2. Mapping các trường quan trọng

| Trường | Mobile idx | PC idx | Vai trò |
|---|---|---|---|
| `raw_time` | 0 | 0 | Thời gian request |
| `raw_domain` | 8 | 8 | Domain publisher |
| `user_id` | **11** | **13** | Cookie tracking ID |
| `client_ip` (IPv4) | **23** | **19** | IP người dùng (lookup GeoIP) |
| `client_ip` (IPv6, ít dùng) | 25 | 26 | Fallback |
| `session_id` | 24 | 24 | Session |

> ⚠️ **Lưu ý quan trọng:** PC có 29 trường (nhiều hơn Mobile 1 trường), nên index của `user_id` và `client_ip` **bị lệch**. Code cũ từng bị bug vì dùng cùng index cho cả 2 nền tảng.

### 3.3. Output (gold)

```
file:///data/softs/datnt_workspace/output/gold/<YYYY-MM-DD>/
├── dim_time/                # 24 rows (24h)
├── dim_date/                # theo range ngày
├── dim_platform/            # 2 rows (Mobile/PC)
├── dim_domain/              # unique domains
├── dim_location/            # IP → city/country/region/lat/lon
└── fact_pageview_hourly/    # partitioned by platform_key
    ├── platform_key=1/      # Mobile
    └── platform_key=2/      # PC
```

Và CSV cho analyst (1 file/ngày trong `output/<YYYY-MM-DD>/`):

- `bai1_top10_{mobile,pc}_overall.csv` — top 10 domain tổng
- `bai1_top10_{mobile,pc}_daily.csv` — top 10 domain theo ngày
- `bai2_geo_top1_{mobile,pc}_country.csv` — top country
- `bai2_geo_top1_{mobile,pc}_city.csv` — top city
- `bai3_{mobile,pc}_hourly_pattern.csv` — pattern 24h

---

## 4. Kiến trúc star schema

```
                 ┌──────────────┐
                 │   dim_time   │
                 │  time_key    │
                 │  hour_of_day │
                 │  time_slot   │
                 │  period_of_day│
                 └──────┬───────┘
                        │
┌──────────────┐        │        ┌──────────────┐
│ dim_platform │        │        │  dim_domain  │
│ platform_key ├────────┼────────┤ domain_key   │
│ platform_name│        │        │ clean_domain │
└──────┬───────┘        │        └──────┬───────┘
       │                │               │
       │       ┌────────▼─────────┐     │
       └──────►│fact_pageview_    │◄────┘
               │   hourly         │
               │ - time_key      │
               │ - date_key      │
               │ - platform_key  │
               │ - domain_key    │
               │ - location_key  │
               │ - user_key      │
               │ - view_count    │
               │ - distinct_users│
               └────────┬────────┘
                        │
                 ┌──────▼───────┐
                 │ dim_location │
                 │ location_key │
                 │ city/country │
                 │ lat/lon      │
                 └──────────────┘
```

---

## 5. Pipeline tổng quan

```
[Raw Parquet trên HDFS]
        │
        ▼
[parse_platform] ─── split by \t ─── chọn 4 cột: time, domain, user_id, ip
        │                                            │
        │                                            ▼
        │                                    [GeoIP lookup]
        │                                            │
        │                                            ▼
        │                                    [dim_location]
        │                                            │
        ▼                                            │
[union Mobile + PC]                                  │
        │                                            │
        ├──► [dim_time]  (24h)                      │
        ├──► [dim_date]  (ngày trong khoảng)        │
        ├──► [dim_platform] (2 rows)                │
        ├──► [dim_domain] (unique domains)          │
        │                                            │
        ▼                                            ▼
[fact_pageview_hourly] ◄─────── join 5 dim ─────────┘
        │
        ▼
[4 bài toán phân tích] ──► CSV
```

---

## 6. Các bài toán phân tích

| # | Tên | Mô tả |
|---|---|---|
| 1 | Top 10 domain | Top 10 domain được xem nhiều nhất (overall + theo ngày) |
| 2 | Geo top 1 | Quốc gia / thành phố có traffic cao nhất theo IP |
| 3 | Hourly pattern | Phân bố lượt xem theo 24h (peak hour) |

---

## 7. Cách chạy

### 7.1. Chạy 1 ngày (test)

```bash
SPARK_LOCAL_IP=10.5.94.214 spark-submit \
    --master spark://10.5.94.214:17077 \
    ~/bigdata_project/build_kimball_v3.py
```

### 7.2. Chạy nhiều ngày nền (background)

```bash
nohup ./run_10days.sh > /dev/null 2>&1 &
disown
```

Xem log:

```bash
tail -f ~/bigdata_project/output/run_10days.log
```

---

## 8. Cấu hình Spark

| Tham số | Giá trị | Lý do |
|---|---|---|
| `spark.driver.memory` | 6g | Cache dim + collect nhiều |
| `spark.executor.memory` | 6g | Xử lý 1 ngày ~vài GB parquet |
| `spark.executor.cores` | 2 | Cluster có ~40 cores / 21 executor |
| `spark.eventLog.enabled` | false | Tiết kiệm disk |

---

## 9. Code chính

| File | Vai trò |
|---|---|
| `build_kimball_v3.py` | Pipeline chính (parse → 5 dim → fact → 4 bài toán) |
| `run_10days.sh` | Wrapper chạy tuần tự 10 ngày, mỗi ngày 1 folder output |
| `/tmp/check_field_count.py` | Debug: đếm số trường mỗi row |
| `/tmp/debug_pc_full.py` | Debug: in toàn bộ 29 trường của PC |
| `/tmp/verify_fix.py` | Verify: PC IP giờ lấy đúng IPv4 |

---

## 10. Bug đã sửa

### Bug: PC lấy nhầm IPv6 thay vì IPv4

**Triệu chứng:** Sau khi chạy pipeline, `dim_location` cho platform PC gần như rỗng (GeoIP lookup fail vì IP là IPv6 ở format `2402:800:...`).

**Nguyên nhân:** Code cũ dùng `ip_col=26` cho cả Mobile và PC. Nhưng:
- Mobile có 28 trường → IP thật nằm ở index 23
- PC có 29 trường → IP thật nằm ở index 19 (IPv4) hoặc 26 (IPv6 — field thừa)

**Fix:** Tách biệt parameter khi gọi `parse_platform`:

```python
# build_kimball_v3.py
df_mobile = parse_platform(spark, MOBILE_SILVER, 11, 23, "Mobile")  # user_id=11, ip=23
df_pc     = parse_platform(spark, PC_SILVER,     13, 19, "PC")      # user_id=13, ip=19
```

**Verify sau fix:**

```
=== PC with new mapping (13=user_id, 19=ip) ===
  time=2026-09-02 20:58:05  domain=thethaovanhoa.vn  user_id=5088357440972582742  client_ip=192.168.6.181
  time=2026-09-02 20:58:05  domain=bongdaplus.vn     user_id=8572826112738912581  client_ip=192.168.6.179
  time=2026-09-02 20:58:05  domain=batdongsan.com.vn user_id=8200575241907968140  client_ip=172.18.1.76
```

→ IP sạch, lookup GeoIP thành công.

---

## 11. Hạn chế & hướng phát triển

| Hạn chế | Hướng cải tiến |
|---|---|
| Chạy tuần tự 10 ngày (chậm) | Chạy song song 10 job, mỗi ngày 1 SparkSession |
| Output ở local FS | Đẩy gold lên HDFS hoặc Hive table |
| `.cache()` không có `.unpersist()` ở cuối | Thêm cleanup để tiết kiệm RAM khi scale |
| Chỉ hỗ trợ 1 quốc gia (VN) trong GeoIP | Mở rộng bằng cách thêm nhiều mmdb region |
| Không có data quality check (great-expectations) | Thêm validate schema, range, null % |

---

---

## 12. Tác giả & thông tin

- **Project**: Adt Platform — Big Data Analytics
- **Data range**: 2026-09-02 → 2026-09-11 (10 ngày)
---

## 13. Bài 4: Visualization Dashboard (Power BI / Metabase Integration)

Dự án cung cấp bộ công cụ trực quan hóa hoàn chỉnh đáp ứng trọn vẹn yêu cầu Bài 4:

1. **Web BI Dashboard tương tác cao cấp (`index.html`)**:
   - Được xây dựng với phong cách Dark Glassmorphism hiện đại.
   - Trực quan hóa toàn bộ 35.5M records qua các biểu đồ: Top Domains (Bài 1), Geo Distribution (Bài 2), 24h Hourly Curve (Bài 3), và sơ đồ Kimball ERD.
   - **SQL Studio**: Công cụ viết truy vấn SQL trực tiếp mô phỏng Metabase/Power BI, hỗ trợ chạy query, xem bảng dữ liệu và tải CSV.
2. **Kho dữ liệu quan hệ cho BI**:
   - File SQLite: `/data/softs/datnt_workspace/output/gold/kimball_gold.db`
   - Parquet Star Schema: `/data/softs/datnt_workspace/output/gold/`
3. **Tài liệu hướng dẫn kết nối Metabase & Power BI**:
   - Xem chi tiết tại [METABASE_SETUP_GUIDE.md](file:///c:/download/Dashboard/METABASE_SETUP_GUIDE.md) với đầy đủ câu lệnh Docker khởi chạy Metabase và danh mục câu lệnh SQL chuẩn.

