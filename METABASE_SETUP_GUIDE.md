# HƯỚNG DẪN CẤU HÌNH METABASE & POWER BI CHO KHO DỮ LIỆU KIMBALL GOLD

> **Dự án**: Adt Platform — Big Data Analytics  
> **Tầng Gold**: Kho dữ liệu Kimball Star Schema (`dim_time`, `dim_date`, `dim_platform`, `dim_domain`, `dim_location`, `fact_pageview_hourly`)  
> **Database File**: `/data/softs/datnt_workspace/output/gold/kimball_gold.db`  
> **Parquet Files**: `file:///data/softs/datnt_workspace/output/gold/`

---

## 1. Giới thiệu giải pháp BI (Visualization Dashboard)

Để đáp ứng yêu cầu **"Dashboard trực quan hóa có khả năng viết query SQL linh hoạt như Power BI"**, hệ thống cung cấp 2 giải pháp song hành:

1. **Web BI Dashboard tích hợp sẵn (`index.html`)**:
   - Chạy ngay lập tức, không cần cài đặt backend phức tạp.
   - Giao diện Dark Glassmorphism cao cấp chuẩn Executive.
   - Tích hợp sẵn **SQL Studio** mô phỏng Metabase/Power BI: cho phép viết câu lệnh SQL trực tiếp, chạy query, xem kết quả dạng bảng và xuất CSV.
   - Đầy đủ visual chart: Donut Chart, Multi-series Bar Chart, Line Curve 24h, Star Schema ERD.

2. **Metabase BI Server**:
   - Công cụ BI mã nguồn mở phổ biến nhất, hỗ trợ viết native SQL query, drag-and-drop dashboard, bộ lọc slicer giống hệt Power BI.
   - Kết nối trực tiếp vào file database SQLite `kimball_gold.db` đã được khởi tạo sẵn ở tầng Gold.

---

## 2. Hướng dẫn triển khai Metabase trên Server Linux

### Cách 1: Khởi chạy bằng Docker (Khuyến nghị - Nhanh nhất)

Chạy lệnh sau trên terminal server `adt-platform-dev-94-214`:

```bash
docker run -d -p 3000:3000 \
  -v /data/softs/datnt_workspace/output/gold:/gold_data \
  --name metabase_kimball \
  metabase/metabase:latest
```

*Giải thích*:
- Mount thư mục chứa tầng Gold `/data/softs/datnt_workspace/output/gold` vào container tại `/gold_data`.
- Cổng dịch vụ: `http://10.5.94.214:3000`.

### Cách 2: Khởi chạy bằng Java JAR (Nếu server không có Docker)

Tải file `metabase.jar` và chạy nền:

```bash
mkdir -p /home/datnt/metabase && cd /home/datnt/metabase
wget https://downloads.metabase.com/v0.49.0/metabase.jar

nohup java -jar metabase.jar > metabase.log 2>&1 &
```

---

## 3. Cấu hình kết nối Metabase vào Kimball Gold Database

1. Truy cập trình duyệt vào `http://10.5.94.214:3000` (hoặc `http://localhost:3000`).
2. Hoàn tất các bước khởi tạo tài khoản Admin ban đầu.
3. Tại bước **Add your data** (hoặc vào **Admin Settings > Databases > Add Database**):
   - **Database type**: Chọn `SQLite`
   - **Display name**: `Kimball Ad Pageview Warehouse`
   - **Filename**:
     - Nếu dùng Docker: `/gold_data/kimball_gold.db`
     - Nếu dùng JAR: `/data/softs/datnt_workspace/output/gold/kimball_gold.db`
4. Bấm **Save database**. Metabase sẽ tự động quét schema và hiển thị các bảng:
   - `fact_pageview_hourly`
   - `dim_domain`
   - `dim_platform`
   - `dim_time`
   - `dim_location`
   - `dim_date`
   - `bai1_top_domains`
   - `bai2_geo_cities`
   - `bai3_hourly_patterns`

---

## 4. Danh mục câu lệnh SQL chuẩn (Power BI / Metabase Native Queries)

Dưới đây là các câu lệnh SQL viết sẵn để tạo các câu hỏi (Questions) và ghim vào Dashboard:

### 4.1. Bài 1: Top 10 Domains nhiều lượt xem nhất (Overall & Platform)

```sql
-- Top 10 domain tổng hợp toàn bộ 10 ngày
SELECT 
    d.clean_domain AS "Domain",
    p.platform_name AS "Nền tảng",
    SUM(f.pageview_count) AS "Tổng Lượt Xem",
    ROUND(SUM(f.pageview_count) * 100.0 / 35504320, 2) AS "Thị Phần (%)"
FROM fact_pageview_hourly f
JOIN dim_domain d ON f.domain_key = d.domain_key
JOIN dim_platform p ON f.platform_key = p.platform_key
GROUP BY d.clean_domain, p.platform_name
ORDER BY SUM(f.pageview_count) DESC
LIMIT 10;
```
*Visual khuyến nghị trong Metabase*: **Row / Bar Chart** (Trục X: Tổng Lượt Xem, Trục Y: Domain, Phân loại theo Nền tảng).

---

### 4.2. Bài 2: Phân bố Địa lý theo IP (Thành phố & Quốc gia)

```sql
-- Phân bố lưu lượng theo tỉnh thành phố (Geo City)
SELECT 
    l.city AS "Thành phố",
    l.country AS "Quốc gia",
    SUM(f.pageview_count) AS "Lượt Xem",
    ROUND(SUM(f.pageview_count) * 100.0 / (SELECT SUM(pageview_count) FROM fact_pageview_hourly), 2) AS "Tỷ Trọng (%)"
FROM fact_pageview_hourly f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.city, l.country
ORDER BY SUM(f.pageview_count) DESC;
```
*Visual khuyến nghị trong Metabase*: **Pie / Donut Chart** hoặc **Progress Bar**.

---

### 4.3. Bài 3: Hourly Traffic Pattern 24h (Phát hiện Giờ Cao Điểm Mobile vs PC)

```sql
-- Phân bố lượt xem theo 24 giờ và so sánh hành vi giữa Mobile và PC
SELECT 
    t.hour_of_day AS "Khung Giờ (H)",
    t.time_slot AS "Khoảng Thời Gian",
    t.period_of_day AS "Buổi trong ngày",
    SUM(CASE WHEN f.platform_key = 1 THEN f.pageview_count ELSE 0 END) AS "Lượt xem Mobile",
    SUM(CASE WHEN f.platform_key = 2 THEN f.pageview_count ELSE 0 END) AS "Lượt xem PC",
    SUM(f.pageview_count) AS "Tổng Lượt Xem"
FROM fact_pageview_hourly f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY t.hour_of_day, t.time_slot, t.period_of_day
ORDER BY t.hour_of_day ASC;
```
*Visual khuyến nghị trong Metabase*: **Line Chart** (Trục X: Khung Giờ, Trục Y: Lượt xem Mobile và Lượt xem PC).

---

### 4.4. Thẻ KPI Tổng hợp (Executive KPI Cards)

```sql
-- Tổng số pageviews
SELECT SUM(pageview_count) AS "Total Pageviews" FROM fact_pageview_hourly;

-- Tỷ lệ đóng góp Mobile vs PC
SELECT 
    p.platform_name AS "Platform",
    SUM(f.pageview_count) AS "Pageviews",
    ROUND(SUM(f.pageview_count) * 100.0 / (SELECT SUM(pageview_count) FROM fact_pageview_hourly), 1) AS "Tỷ Lệ (%)"
FROM fact_pageview_hourly f
JOIN dim_platform p ON f.platform_key = p.platform_key
GROUP BY p.platform_name;
```

---

## 5. Kết nối trực tiếp từ Power BI Desktop (Nếu dùng Power BI)

Nếu bạn muốn mở trực tiếp trên phần mềm **Microsoft Power BI Desktop**:

1. Tải file SQLite `/data/softs/datnt_workspace/output/gold/kimball_gold.db` về máy Windows của bạn (hoặc kết nối qua ODBC driver).
2. Trong Power BI Desktop:
   - Chọn **Get Data > More... > ODBC** (hoặc chọn **SQLite/PostgreSQL/MySQL** nếu đẩy lên RDBMS).
   - Dùng driver: `SQLite3 ODBC Driver`.
   - Chuỗi kết nối: `DRIVER={SQLite3 ODBC Driver};Database=C:\download\Dashboard\kimball_gold.db;`.
3. Hoặc nhanh nhất: Dùng tính năng **Get Data > Folder / Parquet** và trỏ trực tiếp vào thư mục Parquet tầng Gold:
   - `dim_time`
   - `dim_platform`
   - `dim_location`
   - `dim_domain`
   - `dim_date`
   - `fact_pageview_hourly`
4. Power BI sẽ tự động nhận diện các quan hệ khóa (Relationship) thông qua `time_key`, `platform_key`, `location_key`, `domain_key`, `date_key`.

---

## 6. Sử dụng Web Dashboard có sẵn (`index.html`)

Nếu không muốn cài đặt thêm Metabase hoặc Power BI, bạn có thể sử dụng ngay ứng dụng Web Dashboard được cung cấp sẵn tại thư mục hiện tại:

- Mở file `c:\download\Dashboard\index.html` bằng bất kỳ trình duyệt nào (Chrome, Edge, Firefox).
- Ứng dụng đã được nạp sẵn toàn bộ dữ liệu 10 ngày (35.5M records) từ kho Kimball Gold.
- Chức năng:
  1. **Tab Tổng quan Executive**: Thẻ KPI, cấu trúc thị phần, thống kê nhanh.
  2. **Tab Bài 1**: Bảng xếp hạng Top Domains (Vietjack, Kenh14, Tuoitre, Thuvienphapluat...).
  3. **Tab Bài 2**: Bản đồ phân bố địa lý (Hà Nội 62.4%, TP.HCM 28.1%).
  4. **Tab Bài 3**: Biểu đồ 24h đối sánh Mobile (đỉnh 20:00) vs PC (đỉnh 10:00).
  5. **Tab Mô hình Star Schema (ERD)**: Sơ đồ tương tác thực thể - quan hệ chuẩn Kimball.
  6. **Tab SQL Studio**: Trình soạn thảo và thực thi câu lệnh SQL trực tiếp như Metabase, cho phép tải kết quả ra file CSV.
