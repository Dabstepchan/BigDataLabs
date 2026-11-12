"""Часть 8: прогноз температуры сенсора (LightGBM с лагами), квантили, рекурсивная и прямая стратегии,
решающий слой автоотключения из четырёх условий и метрики раннего предупреждения."""
import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor

URL = ('https://raw.githubusercontent.com/numenta/NAB/master/data/realKnownCause/'
       'ambient_temperature_system_failure.csv')
FAILURES = [(pd.Timestamp('2013-12-15'), pd.Timestamp('2013-12-30 23:00')),
            (pd.Timestamp('2014-03-29'), pd.Timestamp('2014-04-20 23:00'))]
LAGS = [1, 2, 3, 6, 12, 24, 48, 168]
QUANTILES = [0.01, 0.05, 0.1, 0.5, 0.9, 0.95, 0.99]


def load(src=URL):
    df = pd.read_csv(src, parse_dates=['timestamp']).set_index('timestamp').asfreq('h')
    df['observed'] = df['value'].notna()
    df['y'] = df['value'].interpolate('time')     # для признаков; метрики считаются только по наблюдениям
    return df


def features_from_array(a, t_index):
    """Признаки момента t по истории a[..t] (a[-1] = значение в момент t)."""
    n = len(a)
    row = {f'lag_{k - 1}': a[n - k] for k in LAGS}
    row.update({'lag_0': a[-1], 'mean_24': a[-24:].mean(), 'std_24': a[-24:].std(), 'mean_168': a[-168:].mean(),
                'diff_1': a[-1] - a[-2], 'diff_24': a[-1] - a[-25],
                'hour': t_index.hour, 'dow': t_index.dayofweek})
    return row


def make_features(y):
    """Таблица признаков для всех моментов: только прошлые и текущее значения, без заглядывания вперёд."""
    X = pd.DataFrame(index=y.index)
    X['lag_0'] = y
    for k in LAGS:
        X[f'lag_{k - 1}'] = y.shift(k - 1)
    X['mean_24'] = y.rolling(24).mean()
    X['std_24'] = y.rolling(24).std(ddof=0)
    X['mean_168'] = y.rolling(168).mean()
    X['diff_1'] = y.diff(1)
    X['diff_24'] = y.diff(24)
    X['hour'] = y.index.hour
    X['dow'] = y.index.dayofweek
    return X


FEATS = None


def _lgbm(objective='regression', alpha=None, seed=42):
    kw = dict(n_estimators=300, learning_rate=0.03, num_leaves=15, min_child_samples=30, subsample=0.8,
              subsample_freq=1, colsample_bytree=0.8, random_state=seed, verbose=-1, n_jobs=2)
    return LGBMRegressor(objective=objective, alpha=alpha, **kw) if alpha else LGBMRegressor(objective=objective, **kw)


def fit_direct(df, train_end, H, quantiles=QUANTILES, seed=42):
    """Прямая стратегия: отдельные модели на каждый шаг h (точка L2 и квантили)."""
    global FEATS
    X = make_features(df['y'])
    FEATS = list(X.columns)
    models = {}
    for h in range(1, H + 1):
        # цель - приращение y(t+h) - y(t): деревья не выходят за диапазон обучающих целей, а уровень
        # прибавляется к прогнозу приращения, поэтому прогноз может уйти выше максимума обучения
        target = df['y'].shift(-h) - df['y']
        ok = df['observed'].shift(-h, fill_value=False) & X.notna().all(axis=1) & (X.index <= train_end - pd.Timedelta(hours=h))
        Xt, yt = X[ok], target[ok]
        models[(h, 'point')] = _lgbm(seed=seed).fit(Xt, yt)
        for q in quantiles:
            models[(h, q)] = _lgbm('quantile', q, seed).fit(Xt, yt)
    return models, X


def predict_direct(models, X, origins, H, quantiles=QUANTILES):
    """Прогнозы прямой стратегии из каждого момента origins на шаги 1..H: DataFrame (origin, h, point, q...)."""
    Xo = X.loc[origins]
    out = []
    level = Xo['lag_0'].to_numpy()
    for h in range(1, H + 1):
        d = pd.DataFrame({'origin': origins, 'h': h, 'point': level + models[(h, 'point')].predict(Xo)})
        for q in quantiles:
            d[f'q{q}'] = level + models[(h, q)].predict(Xo)
        out.append(d)
    res = pd.concat(out, ignore_index=True)
    qs = [f'q{q}' for q in quantiles]
    res[qs] = np.sort(res[qs].to_numpy(), axis=1)   # квантили не должны пересекаться
    return res


def predict_recursive(models, y, origins, H, q=0.9):
    """Рекурсивная стратегия: одношаговые модели (h=1), прогноз подставляется как факт для следующего шага.
    Квантиль на шаге h - одношаговый квантиль от уже спрогнозированной траектории (неопределённость прошлых
    шагов в нём не накапливается)."""
    m_point, m_q = models[(1, 'point')], models[(1, q)]
    vals = y.to_numpy()
    pos = {t: i for i, t in enumerate(y.index)}
    rows = []
    for o in origins:
        i = pos[o]
        a = list(vals[i - 200:i + 1])
        t = o
        for h in range(1, H + 1):
            xr = pd.DataFrame([features_from_array(np.asarray(a), t)])[FEATS]
            p, pq = a[-1] + m_point.predict(xr)[0], a[-1] + m_q.predict(xr)[0]
            rows.append({'origin': o, 'h': h, 'point': p, f'q{q}': pq})
            a.append(p)
            t = t + pd.Timedelta(hours=1)
    return pd.DataFrame(rows)


def pinball(y, qpred, q):
    d = y - qpred
    return float(np.mean(np.maximum(q * d, (q - 1) * d)))


def horizon_metrics(fc, df, scale, q=0.9):
    """MAE, MASE и pinball по шагам горизонта; учитываются только наблюдённые значения."""
    f = fc.copy()
    f['target_time'] = f['origin'] + pd.to_timedelta(f['h'], unit='h')
    f = f.join(df[['value', 'observed']], on='target_time')
    f = f[f['observed']]
    g = f.groupby('h')
    res = pd.DataFrame({'MAE': g.apply(lambda d: np.mean(np.abs(d['value'] - d['point'])))})
    res['MASE'] = res['MAE'] / scale
    res[f'pinball_q{q}'] = g.apply(lambda d: pinball(d['value'].to_numpy(), d[f'q{q}'].to_numpy(), q))
    res[f'coverage_q{q}'] = g.apply(lambda d: float(np.mean(d['value'] <= d[f'q{q}'])))
    return res


def exceed_probability(fc, t_crit, H, quantiles=QUANTILES, upper=True):
    """P(T > T_крит хотя бы на одном шаге 1..H) по квантильному прогнозу: для каждого шага уровень,
    на котором квантиль пересекает T_крит (линейная интерполяция), берётся максимум по горизонту."""
    lv = np.array(quantiles)
    qs = fc[[f'q{q}' for q in quantiles]].to_numpy()
    p = np.empty(len(fc))
    for i, row in enumerate(qs):
        if upper:
            p[i] = 1 - np.interp(t_crit, row, lv, left=0.0, right=1.0)
        else:
            p[i] = np.interp(t_crit, row, lv, left=0.0, right=1.0)
    f = fc[['origin', 'h']].copy()
    f['p'] = p
    return f[f['h'] <= H].groupby('origin')['p'].max()


def cusum(resid, k=0.5, upper=True):
    """Односторонний CUSUM по стандартизованным остаткам (факт - прогноз)."""
    s, out = 0.0, []
    for r in resid:
        r = r if upper else -r
        s = max(0.0, s + (0.0 if np.isnan(r) else r) - k)
        out.append(s)
    return np.array(out)


def alarm_log(times, prob, cus, p_star, h_cusum, k_persist, cooldown_h):
    """Решающий слой: тревога, только если одновременно P > p*, CUSUM > порога, нарушение держится
    k выборок подряд и не активен cooldown. Возвращает моменты тревог."""
    alarms, persist, last = [], 0, None
    for t, p, c in zip(times, prob, cus):
        viol = (p > p_star) and (c > h_cusum)
        persist = persist + 1 if viol else 0
        cool = last is not None and (t - last) < pd.Timedelta(hours=cooldown_h)
        if persist >= k_persist and not cool:
            alarms.append(t)
            last = t
    return alarms


def ladder(alarms, prob, times, p_shed=0.5, p_shutdown=0.9):
    """Лестница действий для каждой тревоги: уведомить -> сбросить нагрузку -> контролируемое отключение."""
    pr = pd.Series(prob, index=times)
    rows = []
    for a in alarms:
        nxt = pr[a:a + pd.Timedelta(hours=6)]
        act = 'уведомить'
        if (nxt > p_shed).sum() >= 3:
            act = 'сбросить нагрузку'
        if (nxt > p_shutdown).sum() >= 3:
            act = 'контролируемое отключение'
        rows.append({'time': a, 'P_max_6h': float(nxt.max()), 'action': act})
    return pd.DataFrame(rows)


def early_warning_metrics(alarms, failures, start, end, w_hours=24):
    """Lead time, precision/recall с окном допуска ±W, ложные тревоги в день, пропущенные отказы."""
    W = pd.Timedelta(hours=w_hours)
    fails = [(a, b) for a, b in failures if b >= start and a <= end]
    tp_alarms, lead, missed = set(), [], 0
    for a, b in fails:
        hit = [t for t in alarms if a - W <= t <= b + W]
        tp_alarms.update(hit)
        if hit:
            lead.append((a - min(hit)).total_seconds() / 3600)
        else:
            missed += 1
    fp = [t for t in alarms if t not in tp_alarms]
    n_days = (end - start).total_seconds() / 86400
    return {'alarms': len(alarms), 'true_alarms': len(tp_alarms), 'false_alarms': len(fp),
            'precision': len(tp_alarms) / len(alarms) if alarms else np.nan,
            'recall': (len(fails) - missed) / len(fails) if fails else np.nan,
            'false_per_day': len(fp) / n_days, 'missed_failures': missed, 'failures': len(fails),
            'lead_time_h': float(np.mean(lead)) if lead else np.nan}
