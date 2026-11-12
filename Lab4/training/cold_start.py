"""Обработка холодного старта (вариант 5).

Холодный пользователь - без единого события за 7 дней до момента запроса: в Feature Store для него нет
снимка, и все пользовательские фичи пусты. Наивный вариант (ошибка из лекции) - заполнить их нулями.
Здесь вместо этого:
1) флаг is_cold, чтобы модель отличала «нет истории» от «смотрел, но не покупал»;
2) априорные значения: медианы фич у пользователей с минимальной историей (1 активный день) вместо нулей;
3) feature dropout при обучении: у части тёплых пользователей фичи скрываются, и модель учится на «холодных»
   примерах из тех же сессий;
4) для рекомендаций без истории - популярное в категории текущего товара, а не глобальный топ.
"""
import numpy as np
import pandas as pd

from .data import USER_FEATURES


def is_cold(df):
    return df['u_views_7d'].isna() | (df['u_views_7d'] <= 0)


def user_priors(train):
    """Априорные значения фич пользователя: медианы у тех, кто был активен ровно 1 день из 7."""
    newbies = train[train['u_active_days_7d'] == 1]
    return {c: float(newbies[c].median()) for c in USER_FEATURES}


def baseline_fill(df):
    """Наивная обработка: пустые пользовательские фичи заполняются нулями."""
    out = df.copy()
    out[USER_FEATURES] = out[USER_FEATURES].fillna(0.0)
    return out


def cold_fill(df, priors):
    """Обработка холодного старта: флаг и априорные значения вместо нулей."""
    out = df.copy()
    out['is_cold'] = is_cold(out).astype(float)
    for c in USER_FEATURES:
        out[c] = out[c].where(~out['is_cold'].astype(bool), priors[c])
    return out


def feature_dropout(train, priors, frac=0.15, seed=42):
    """Копия части тёплых строк, где пользовательские фичи скрыты (как у холодного пользователя)."""
    rng = np.random.default_rng(seed)
    warm = train[~is_cold(train)]
    idx = rng.choice(len(warm), size=int(len(warm) * frac), replace=False)
    aug = warm.iloc[idx].copy()
    aug[USER_FEATURES] = np.nan
    return pd.concat([train, aug], ignore_index=True)


def category_fallback(product_daily, as_of, k=10):
    """Популярные товары по категориям за 7 дней до as_of: список рекомендаций для пользователя без истории."""
    t = pd.Timestamp(as_of)
    w = product_daily[(pd.to_datetime(product_daily['day']) >= t - pd.Timedelta(days=7)) &
                      (pd.to_datetime(product_daily['day']) < t)]
    agg = (w.groupby(['category_id', 'product_id'], as_index=False)[['purchases', 'carts', 'views']].sum()
             .sort_values(['category_id', 'purchases', 'carts', 'views'], ascending=[True, False, False, False]))
    by_cat = agg.groupby('category_id').head(k).groupby('category_id')['product_id'].apply(list).to_dict()
    glob = (agg.sort_values(['purchases', 'carts', 'views'], ascending=False).head(k)['product_id'].tolist())
    return by_cat, glob
