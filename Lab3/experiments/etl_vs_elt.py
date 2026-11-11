"""Одна и та же трансформация двумя способами.

ETL: данные извлекаются в pandas, очищаются и агрегируются в Python, в хранилище грузится только результат.
ELT: сырые строки грузятся в хранилище (DuckDB) как есть, трансформация выполняется SQL внутри него.
Результат обоих способов: популярность товаров в категориях и профиль пользователей.
Запуск: python etl_vs_elt.py {etl|elt} <landing> <дни через запятую> <каталог результата> <лимит памяти DuckDB, МБ>
"""
import glob
import json
import os
import sys
import time

mode, landing, days, out, mem_mb = sys.argv[1:6]
files = sorted(f for d in days.split(',') for f in glob.glob(f'{landing}/dt={d}/hr=*/*.parquet'))
os.makedirs(out, exist_ok=True)
t0 = time.perf_counter()

if mode == 'etl':
    import pandas as pd
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)        # Extract
    rows_in = len(df)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')                      # Transform
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')
    df['user_id'] = pd.to_numeric(df['user_id'], errors='coerce')
    df['category_id'] = pd.to_numeric(df['category_id'], errors='coerce')
    df = df[(df['price'] > 0) & df['user_session'].notna()].drop_duplicates()
    df['brand'] = df['brand'].fillna('unknown')
    pop = (df.assign(p=df['event_type'].eq('purchase'), c=df['event_type'].eq('cart'))
             .groupby(['category_id', 'product_id'], as_index=False)[['p', 'c']].sum()
             .sort_values(['category_id', 'p', 'c', 'product_id'], ascending=[True, False, False, True]))
    pop['rank'] = pop.groupby('category_id').cumcount() + 1
    pop = pop[pop['rank'] <= 10]
    df['revenue'] = df['price'].where(df['event_type'].eq('purchase'), 0.0)
    users = df.groupby('user_id').agg(n_events=('event_type', 'size'), revenue=('revenue', 'sum'))
    pop.to_parquet(f'{out}/item_popularity.parquet')                               # Load
    users.to_parquet(f'{out}/dim_users.parquet')
    rows_out = len(pop) + len(users)
else:
    import duckdb
    con = duckdb.connect(f'{out}/elt.duckdb')
    con.execute(f"SET memory_limit='{mem_mb}MB'; SET preserve_insertion_order=false")
    con.execute('CREATE OR REPLACE TABLE raw AS SELECT * FROM read_parquet(?)', [files])   # Extract + Load
    rows_in = con.execute('SELECT count(*) FROM raw').fetchone()[0]
    con.execute("""CREATE OR REPLACE TABLE stg AS SELECT DISTINCT                               -- Transform
                       event_type, TRY_CAST(product_id AS BIGINT) AS product_id,
                       TRY_CAST(category_id AS BIGINT) AS category_id, coalesce(brand, 'unknown') AS brand,
                       TRY_CAST(price AS DOUBLE) AS price, TRY_CAST(user_id AS BIGINT) AS user_id,
                       user_session, event_time
                   FROM raw WHERE TRY_CAST(price AS DOUBLE) > 0 AND user_session IS NOT NULL""")
    con.execute("""CREATE OR REPLACE TABLE item_popularity AS SELECT * FROM (
                       SELECT category_id, product_id, count(*) FILTER (WHERE event_type = 'purchase') AS p,
                              count(*) FILTER (WHERE event_type = 'cart') AS c
                       FROM stg GROUP BY ALL)
                   QUALIFY row_number() OVER (PARTITION BY category_id ORDER BY p DESC, c DESC, product_id) <= 10""")
    con.execute("""CREATE OR REPLACE TABLE dim_users AS
                   SELECT user_id, count(*) AS n_events,
                          coalesce(sum(price) FILTER (WHERE event_type = 'purchase'), 0) AS revenue
                   FROM stg GROUP BY user_id""")
    rows_out = con.execute('SELECT (SELECT count(*) FROM item_popularity) + (SELECT count(*) FROM dim_users)').fetchone()[0]
    con.close()

print(json.dumps({'mode': mode, 'rows_in': rows_in, 'rows_out': int(rows_out),
                  'seconds': time.perf_counter() - t0}))
