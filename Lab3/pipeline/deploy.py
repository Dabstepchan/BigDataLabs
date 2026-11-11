"""CI/CD модели рекомендаций: сборка версии в staging, smoke-тест, canary, продвижение и откат."""
import json
import os
import time

import duckdb
import numpy as np

from . import storage as st
from . import transform as tr
from .common import now_utc, send_alert


def build_version(paths, model, warehouse, as_of, batch_start):
    """Сборка версии модели в отдельном хранилище (staging): dbt build со всеми тестами."""
    res = tr.run_dbt(paths, ['build'], tr.model_vars(paths, as_of, batch_start, model), warehouse=warehouse,
                     check=False)
    failed = [r for r in res['results'] if r['status'] in ('fail', 'error')]
    return {'ok': res['returncode'] == 0, 'nodes': len(res['results']), 'failed': failed}


def smoke_test(warehouse, k=10, n_lookups=300, seed=42, latency_ms=100.0, min_coverage=0.99):
    """Есть рекомендации, у пользователей полный список из k товаров, поиск по user_id укладывается в SLA."""
    con = duckdb.connect(warehouse, read_only=True)
    try:
        users, rows, full = con.execute(f"""
            SELECT count(DISTINCT user_id), count(*),
                   count(*) FILTER (WHERE n = {k}) FROM (
                SELECT user_id, count(*) AS n FROM user_recommendations GROUP BY user_id)""").fetchone()
        ids = [r[0] for r in con.execute('SELECT DISTINCT user_id FROM user_recommendations ORDER BY user_id').fetchall()]
        rng = np.random.default_rng(seed)
        sample = rng.choice(ids, size=min(n_lookups, len(ids)), replace=False) if ids else []
        lat = []
        for uid in sample:
            t = time.perf_counter()
            con.execute('SELECT product_id FROM user_recommendations WHERE user_id = ? ORDER BY rank',
                        [int(uid)]).fetchall()
            lat.append((time.perf_counter() - t) * 1000)
    finally:
        con.close()
    coverage = full / users if users else 0.0
    p50, p99 = (float(np.percentile(lat, 50)), float(np.percentile(lat, 99))) if lat else (np.nan, np.nan)
    return {'users': users, 'rows': rows, 'full_list_share': coverage, 'lookup_p50_ms': p50, 'lookup_p99_ms': p99,
            'passed': bool(users > 0 and coverage >= min_coverage and p99 < latency_ms)}


def hit_rate(landing, warehouse, as_of, horizon_hours=24, canary_share=None):
    """HitRate@k: доля пользователей, которые в следующие horizon_hours часов взаимодействовали
    хотя бы с одним рекомендованным товаром (any - любое событие, purchase - покупка)."""
    con = duckdb.connect(warehouse, read_only=True)
    try:
        filt = f'WHERE user_id % 100 < {canary_share}' if canary_share else ''
        con.execute(f'CREATE TEMP TABLE r AS SELECT user_id, product_id FROM user_recommendations {filt}')
        con.execute(f"""
            CREATE TEMP TABLE f AS
            SELECT DISTINCT TRY_CAST(user_id AS BIGINT) AS user_id, TRY_CAST(product_id AS BIGINT) AS product_id,
                   event_type
            FROM read_parquet('{landing}/dt=*/hr=*/*.parquet', hive_partitioning=false, union_by_name=true)
            WHERE event_time >= ? AND event_time < ?""",
                    [f'{as_of} UTC', f'{_shift(as_of, horizon_hours)} UTC'])
        users, hit_any, hit_buy = con.execute("""
            SELECT count(DISTINCT r.user_id),
                   count(DISTINCT r.user_id) FILTER (WHERE f.user_id IS NOT NULL),
                   count(DISTINCT r.user_id) FILTER (WHERE f.event_type = 'purchase')
            FROM r LEFT JOIN f USING (user_id, product_id)""").fetchone()
    finally:
        con.close()
    return {'users': users, 'hit_rate_any': hit_any / users if users else 0.0,
            'hit_rate_purchase': hit_buy / users if users else 0.0}


def _shift(ts, hours):
    from datetime import datetime, timedelta
    return (datetime.strptime(ts, '%Y-%m-%d %H:%M:%S') + timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')


def canary(paths, prod_wh, cand_wh, as_of, share=5, tolerance=0.0):
    """Canary: новая версия обслуживает share% пользователей (user_id % 100 < share);
    решение принимается по HitRate на этих же пользователях у текущей и новой версии."""
    prod = hit_rate(paths.landing, prod_wh, as_of, canary_share=share)
    cand = hit_rate(paths.landing, cand_wh, as_of, canary_share=share)
    promote = cand['hit_rate_any'] >= prod['hit_rate_any'] - tolerance
    return {'share_percent': share, 'production': prod, 'candidate': cand,
            'delta_hit_rate_any': cand['hit_rate_any'] - prod['hit_rate_any'], 'promote': bool(promote)}


def model_history(paths):
    f = os.path.join(paths.state, 'model_history.json')
    if os.path.exists(f):
        with open(f) as fh:
            return json.load(fh)
    return []


def promote(paths, model, reason):
    """Продвижение версии в production: запись реестра модели и истории версий."""
    hist = model_history(paths)
    hist.append({'ts': now_utc(), 'action': 'promote', 'model': model, 'reason': reason,
                 'reco_version_before': st.table_version(paths.reco)})
    with open(os.path.join(paths.state, 'model_history.json'), 'w') as fh:
        json.dump(hist, fh, indent=1)
    with open(os.path.join(paths.state, 'production_model.json'), 'w') as fh:
        json.dump(model, fh, indent=1)


def rollback(paths, reason):
    """Автоматический откат: прежняя версия модели в реестре и прежняя версия таблицы сервинга (Delta restore)."""
    hist = model_history(paths)
    last = next((h for h in reversed(hist) if h['action'] == 'promote'), None)
    if last is None:
        return None
    prev = next((h['model'] for h in reversed(hist[:hist.index(last)]) if h['action'] == 'promote'),
                tr.PRODUCTION_MODEL)
    with open(os.path.join(paths.state, 'production_model.json'), 'w') as fh:
        json.dump(prev, fh, indent=1)
    restored_to = last['reco_version_before']
    if restored_to >= 0:
        st.restore(paths.reco, restored_to)
    hist.append({'ts': now_utc(), 'action': 'rollback', 'model': prev, 'reason': reason,
                 'reco_restored_to': restored_to})
    with open(os.path.join(paths.state, 'model_history.json'), 'w') as fh:
        json.dump(hist, fh, indent=1)
    send_alert(paths, 'critical', 'rollback', f'откат на модель {prev["model_version"]}: {reason}')
    return {'model': prev, 'reco_restored_to': restored_to}
