import sqlite3, pandas as pd, os
db_path = '/data/softs/datnt_workspace/output/gold/kimball_gold.db'
if os.path.exists(db_path): os.remove(db_path)
conn = sqlite3.connect(db_path)
csv_dir = '/home/datnt/bigdata_project/output'
tables = [('dim_time', 'dim_time.csv'), ('dim_platform', 'dim_platform.csv'), ('dim_location', 'dim_location.csv'), ('dim_domain', 'dim_domain.csv'), ('dim_date', 'dim_date.csv'), ('fact_pageview_hourly', 'fact_pageview_hourly.csv'), ('bai1_top10_mobile_overall', 'bai1_top10_mobile_overall.csv'), ('bai1_top10_pc_overall', 'bai1_top10_pc_overall.csv'), ('bai2_geo_top1_mobile_city', 'bai2_geo_top1_mobile_city.csv'), ('bai3_mobile_hourly_pattern', 'bai3_mobile_hourly_pattern.csv'), ('bai3_pc_hourly_pattern', 'bai3_pc_hourly_pattern.csv')]
for name, file in tables:
    p = os.path.join(csv_dir, file); df = pd.read_csv(p); df.to_sql(name, conn, if_exists='replace', index=False); print(f'Imported {name}: {len(df)} rows')
conn.close(); print('SQLite DB created successfully at:', db_path)
