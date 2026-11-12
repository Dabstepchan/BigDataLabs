"""Инференс-сервис ранжирования рекомендаций (FastAPI).

Фичи берутся из online store Feast, модель - из MLflow Model Registry. Сервис поддерживает canary
(доля трафика на новую модель), shadow mode (новая модель считает параллельно, ответ не меняет)
и автоматический откат canary по ошибкам и задержке.
"""
import hashlib
import json
import os
import threading
import time
from typing import List, Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from training.cold_start import is_cold
from training.data import PRODUCT_FEATURES, USER_FEATURES

app = FastAPI(title='reco-ranker', version='1.0')

STATE = {
    'store': None, 'primary': None, 'canary': None, 'shadow': None, 'canary_percent': 0,
    'features': None, 'by_category': {}, 'global_top': [], 'product_category': {},
    'log_path': None, 'stats': {'requests': 0, 'errors': 0, 'fs_errors': 0},
    'canary_stats': {'requests': 0, 'errors': 0, 'latency_ms': []}, 'max_error_rate': 0.01,
    'max_p95_ms': 100.0, 'rollback_reason': None,
}
_lock = threading.Lock()


def configure(**kw):
    """Настройка сервиса из ноутбука или скрипта запуска: модели, Feature Store, априорные значения."""
    STATE.update(kw)


class PredictRequest(BaseModel):
    user_id: int = Field(..., gt=0)
    product_id: int = Field(..., gt=0)
    price: float = Field(..., gt=0, lt=1_000_000)
    hour: Optional[int] = Field(None, ge=0, le=23)


class RecommendRequest(BaseModel):
    user_id: int = Field(..., gt=0)
    k: int = Field(10, ge=1, le=50)
    context_product_id: Optional[int] = Field(None, gt=0)


def _variant(user_id):
    h = int(hashlib.md5(str(user_id).encode()).hexdigest(), 16) % 100
    return 'canary' if STATE['canary'] is not None and h < STATE['canary_percent'] else 'primary'


def online_features(user_id, product_ids):
    """Фичи пользователя и товаров из online store; при сбое хранилища - априорные значения."""
    rows = [{'user_id': int(user_id), 'product_id': int(p)} for p in product_ids]
    try:
        d = STATE['store'].get_online_features(
            features=[f'user_stats_7d:{c}' for c in USER_FEATURES] + [f'product_stats_7d:{c}' for c in PRODUCT_FEATURES],
            entity_rows=rows).to_dict()
        df = pd.DataFrame(d)
        fs_ok = True
    except Exception:
        STATE['stats']['fs_errors'] += 1
        df = pd.DataFrame(rows)
        for c in USER_FEATURES + PRODUCT_FEATURES:
            df[c] = np.nan
        fs_ok = False
    return df.astype({c: float for c in USER_FEATURES + PRODUCT_FEATURES}), fs_ok


def score(model_entry, df):
    """Каждая версия модели получает фичи с той же предобработкой, что при обучении (prep версии)."""
    model, feats, prep = model_entry['model'], model_entry['features'], model_entry.get('prep')
    X = prep(df) if prep else df
    return model.predict_proba(X[feats])[:, 1]


def _prepare(df, price, hour):
    df['price'] = price
    df['hour'] = float(hour if hour is not None else time.gmtime().tm_hour)
    df['price_ratio'] = (df['price'] / df['p_avg_price_7d'].where(df['p_avg_price_7d'] > 0)).fillna(1.0)
    df['cold'] = is_cold(df)
    return df


def _log(record):
    if STATE['log_path']:
        with _lock, open(STATE['log_path'], 'a') as fh:
            fh.write(json.dumps(record) + '\n')


def _check_canary():
    """Автоматический откат canary: доля ошибок или p95 задержки выше порога."""
    cs = STATE['canary_stats']
    if STATE['canary'] is None or cs['requests'] < 50:
        return
    err = cs['errors'] / cs['requests']
    p95 = float(np.percentile(cs['latency_ms'][-500:], 95)) if cs['latency_ms'] else 0.0
    if err > STATE['max_error_rate'] or p95 > STATE['max_p95_ms']:
        STATE['rollback_reason'] = f'canary: ошибок {err:.1%}, p95 {p95:.1f} мс'
        STATE['canary'], STATE['canary_percent'] = None, 0


@app.get('/health')
def health():
    return {'status': 'ok', 'primary': STATE['primary']['version'] if STATE['primary'] else None,
            'canary': STATE['canary']['version'] if STATE['canary'] else None,
            'canary_percent': STATE['canary_percent'], 'stats': STATE['stats'],
            'rollback_reason': STATE['rollback_reason']}


@app.post('/predict')
def predict(req: PredictRequest):
    t0 = time.perf_counter()
    STATE['stats']['requests'] += 1
    variant = _variant(req.user_id)
    entry = STATE[variant]
    try:
        df, fs_ok = online_features(req.user_id, [req.product_id])
        df = _prepare(df, req.price, req.hour)
        p = float(score(entry, df)[0])
        shadow_p = float(score(STATE['shadow'], df)[0]) if STATE['shadow'] is not None else None
    except Exception as e:
        STATE['stats']['errors'] += 1
        if variant == 'canary':
            STATE['canary_stats']['errors'] += 1
            STATE['canary_stats']['requests'] += 1
            _check_canary()
        raise HTTPException(status_code=500, detail=f'ошибка инференса: {type(e).__name__}')
    ms = (time.perf_counter() - t0) * 1000
    if variant == 'canary':
        STATE['canary_stats']['requests'] += 1
        STATE['canary_stats']['latency_ms'].append(ms)
        _check_canary()
    _log({'user_id': req.user_id, 'product_id': req.product_id, 'variant': variant, 'version': entry['version'],
          'score': p, 'shadow_score': shadow_p, 'cold': bool(df['cold'].iloc[0]), 'ms': ms})
    return {'score': p, 'model_version': entry['version'], 'variant': variant,
            'cold_start': bool(df['cold'].iloc[0]), 'feature_store_ok': fs_ok, 'latency_ms': ms}


@app.post('/recommend')
def recommend(req: RecommendRequest):
    """Кандидаты - популярное в категории текущего товара (или глобальный топ), ранжируются моделью."""
    t0 = time.perf_counter()
    STATE['stats']['requests'] += 1
    cat = STATE['product_category'].get(req.context_product_id) if req.context_product_id else None
    cands = list(STATE['by_category'].get(cat, [])) if cat is not None else []
    cands += [p for p in STATE['global_top'] if p not in cands]
    if not cands:
        raise HTTPException(status_code=404, detail='нет кандидатов')
    df, fs_ok = online_features(req.user_id, cands)
    df = _prepare(df, df['p_avg_price_7d'].fillna(1.0).clip(lower=0.01), None)
    entry = STATE[_variant(req.user_id)]
    df['score'] = score(entry, df)
    top = df.sort_values('score', ascending=False).head(req.k)
    return {'user_id': req.user_id, 'items': top['product_id'].astype(int).tolist(),
            'scores': top['score'].round(5).tolist(), 'cold_start': bool(df['cold'].iloc[0]),
            'model_version': entry['version'], 'latency_ms': (time.perf_counter() - t0) * 1000}
