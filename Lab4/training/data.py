"""Данные модели ранжирования: выборка пользователей, дневные снимки фич и пары «просмотр -> взаимодействие».

Все фичи считаются на начало дня D по окну [D - 7 дней, D): в снимок дня не попадает ничего из самого дня D,
поэтому при обучении нет утечки будущего, а в инференсе доступен тот же снимок.
"""
import os
from datetime import date, timedelta

import duckdb
import pandas as pd

USER_MOD = 20          # в выборку попадает пользователь с user_id % 20 = 0 (5% пользователей)
WINDOW_DAYS = 7
PRIOR_VIEWS = 20       # сглаживание долей товара: (корзины + prior * средняя доля) / (просмотры + prior)

USER_FEATURES = ['u_views_7d', 'u_carts_7d', 'u_purchases_7d', 'u_sessions_7d', 'u_active_days_7d',
                 'u_avg_price_7d', 'u_cart_rate_7d', 'u_purchase_rate_7d']
PRODUCT_FEATURES = ['p_views_7d', 'p_carts_7d', 'p_purchases_7d', 'p_users_7d', 'p_avg_price_7d',
                    'p_cart_rate_7d', 'p_purchase_rate_7d']
REQUEST_FEATURES = ['price', 'hour', 'price_ratio']
FEATURES = USER_FEATURES + PRODUCT_FEATURES + REQUEST_FEATURES


def days(start, n):
    d0 = date.fromisoformat(start)
    return [(d0 + timedelta(i)).isoformat() for i in range(n)]


def build_base(csv, data_dir, start, n_days, mem_mb, threads=2):
    """Один проход по CSV: дневные агрегаты товаров по всем событиям и события выбранных пользователей."""
    os.makedirs(data_dir, exist_ok=True)
    end = (date.fromisoformat(start) + timedelta(n_days)).isoformat()
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{mem_mb}MB'; SET threads={threads}; SET preserve_insertion_order=false")
    window = os.path.join(data_dir, '_window.parquet')
    con.execute(f"""
        COPY (SELECT CAST(strptime(event_time, '%Y-%m-%d %H:%M:%S UTC') AS TIMESTAMP) AS event_time, event_type,
                     product_id, category_id, category_code, brand, price, user_id, user_session
              FROM read_csv('{csv}', header=true,
                            types={{'event_time': 'VARCHAR', 'category_id': 'BIGINT', 'brand': 'VARCHAR'}})
              WHERE event_time >= '{start}' AND event_time < '{end}' AND price > 0 AND user_session IS NOT NULL)
        TO '{window}' (FORMAT parquet, COMPRESSION zstd)""")
    con.execute(f"""
        COPY (SELECT product_id, CAST(event_time AS DATE) AS day,
                     count(*) FILTER (WHERE event_type = 'view') AS views,
                     count(*) FILTER (WHERE event_type = 'cart') AS carts,
                     count(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                     count(DISTINCT user_id) AS users,
                     sum(price) FILTER (WHERE event_type = 'view') AS view_price_sum,
                     arg_max(category_id, event_time) AS category_id,
                     arg_max(category_code, event_time) AS category_code,
                     arg_max(brand, event_time) AS brand
              FROM read_parquet('{window}') GROUP BY ALL)
        TO '{data_dir}/product_daily.parquet' (FORMAT parquet)""")
    con.execute(f"""
        COPY (SELECT * FROM read_parquet('{window}') WHERE user_id % {USER_MOD} = 0 ORDER BY event_time)
        TO '{data_dir}/events_sample.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    con.close()
    os.remove(window)


def build_snapshots(data_dir, snapshot_days):
    """Снимки фич пользователей и товаров на начало каждого дня (источник для Feast)."""
    con = duckdb.connect()
    snaps = ', '.join(f"(DATE '{d}')" for d in snapshot_days)
    con.execute(f"CREATE TEMP TABLE snap(d) AS VALUES {snaps}")
    user = con.execute(f"""
        WITH ud AS (
            SELECT user_id, CAST(event_time AS DATE) AS day,
                   count(*) FILTER (WHERE event_type = 'view') AS views,
                   count(*) FILTER (WHERE event_type = 'cart') AS carts,
                   count(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                   count(DISTINCT user_session) AS sessions,
                   sum(price) FILTER (WHERE event_type = 'view') AS view_price_sum
            FROM read_parquet('{data_dir}/events_sample.parquet') GROUP BY ALL)
        SELECT ud.user_id, CAST(snap.d AS TIMESTAMP) AS event_timestamp, CAST(snap.d AS TIMESTAMP) AS created,
               CAST(sum(views) AS DOUBLE) AS u_views_7d, CAST(sum(carts) AS DOUBLE) AS u_carts_7d,
               CAST(sum(purchases) AS DOUBLE) AS u_purchases_7d, CAST(sum(sessions) AS DOUBLE) AS u_sessions_7d,
               CAST(count(*) AS DOUBLE) AS u_active_days_7d,
               coalesce(sum(view_price_sum) / nullif(sum(views), 0), 0) AS u_avg_price_7d,
               sum(carts) / (sum(views) + 1.0) AS u_cart_rate_7d,
               sum(purchases) / (sum(views) + 1.0) AS u_purchase_rate_7d
        FROM snap JOIN ud ON ud.day >= snap.d - INTERVAL {WINDOW_DAYS} DAY AND ud.day < snap.d
        GROUP BY ud.user_id, snap.d""").df()
    product = con.execute(f"""
        WITH agg AS (
            SELECT pd.product_id, snap.d,
                   sum(views) AS v, sum(carts) AS c, sum(purchases) AS p, sum(users) AS u,
                   sum(view_price_sum) AS ps
            FROM snap JOIN read_parquet('{data_dir}/product_daily.parquet') pd
              ON pd.day >= snap.d - INTERVAL {WINDOW_DAYS} DAY AND pd.day < snap.d
            GROUP BY pd.product_id, snap.d),
        prior AS (SELECT d, sum(c) / sum(v) AS cr, sum(p) / sum(v) AS pr FROM agg GROUP BY d)
        SELECT agg.product_id, CAST(agg.d AS TIMESTAMP) AS event_timestamp, CAST(agg.d AS TIMESTAMP) AS created,
               CAST(v AS DOUBLE) AS p_views_7d, CAST(c AS DOUBLE) AS p_carts_7d, CAST(p AS DOUBLE) AS p_purchases_7d,
               CAST(u AS DOUBLE) AS p_users_7d, coalesce(ps / nullif(v, 0), 0) AS p_avg_price_7d,
               (c + {PRIOR_VIEWS} * prior.cr) / (v + {PRIOR_VIEWS}) AS p_cart_rate_7d,
               (p + {PRIOR_VIEWS} * prior.pr) / (v + {PRIOR_VIEWS}) AS p_purchase_rate_7d
        FROM agg JOIN prior USING (d)""").df()
    con.close()
    return user, product


def build_pairs(data_dir, day_list):
    """Пары (сессия, товар) с просмотром: метка 1, если товар в той же сессии добавлен в корзину или куплен."""
    con = duckdb.connect()
    dl = ', '.join(f"DATE '{d}'" for d in day_list)
    pairs = con.execute(f"""
        WITH s AS (SELECT * FROM read_parquet('{data_dir}/events_sample.parquet')
                   WHERE CAST(event_time AS DATE) IN ({dl})),
        v AS (SELECT user_session, product_id, any_value(user_id) AS user_id, min(event_time) AS event_timestamp,
                     arg_min(price, event_time) AS price
              FROM s WHERE event_type = 'view' GROUP BY user_session, product_id),
        pos AS (SELECT DISTINCT user_session, product_id FROM s WHERE event_type IN ('cart', 'purchase'))
        SELECT v.user_id, v.product_id, v.user_session, v.event_timestamp, v.price,
               CAST(hour(v.event_timestamp) AS DOUBLE) AS hour,
               CAST(pos.product_id IS NOT NULL AS INTEGER) AS label
        FROM v LEFT JOIN pos USING (user_session, product_id)
        ORDER BY v.event_timestamp, v.user_id, v.product_id""").df()
    con.close()
    return pairs


def user_features_direct(data_dir, user_id, as_of):
    """Те же фичи пользователя, посчитанные напрямую в pandas по сырым событиям (для проверки согласованности)."""
    ev = pd.read_parquet(f'{data_dir}/events_sample.parquet', filters=[('user_id', '=', int(user_id))])
    t1 = pd.Timestamp(as_of).normalize()
    w = ev[(ev.event_time >= t1 - pd.Timedelta(days=WINDOW_DAYS)) & (ev.event_time < t1)]
    if w.empty:
        return None
    views = (w.event_type == 'view').sum()
    carts = (w.event_type == 'cart').sum()
    purchases = (w.event_type == 'purchase').sum()
    return {'u_views_7d': float(views), 'u_carts_7d': float(carts), 'u_purchases_7d': float(purchases),
            'u_sessions_7d': float(w.groupby(w.event_time.dt.date).user_session.nunique().sum()),
            'u_active_days_7d': float(w.event_time.dt.date.nunique()),
            'u_avg_price_7d': float(w.loc[w.event_type == 'view', 'price'].mean()) if views else 0.0,
            'u_cart_rate_7d': carts / (views + 1.0), 'u_purchase_rate_7d': purchases / (views + 1.0)}


def add_request_features(df):
    """Признаки запроса: цена просмотра, час и цена относительно средней цены товара."""
    df = df.copy()
    df['price_ratio'] = (df['price'] / df['p_avg_price_7d'].where(df['p_avg_price_7d'] > 0)).fillna(1.0)
    return df
