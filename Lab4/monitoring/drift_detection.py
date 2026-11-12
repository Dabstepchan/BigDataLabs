"""Мониторинг данных и модели: PSI и KS по фичам, пропуски и диапазоны, Evidently, алерты, логика retrain."""
import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

PSI_WARN = 0.1          # умеренный сдвиг
PSI_DRIFT = 0.2         # значимый сдвиг (порог из лекции)
DRIFT_SHARE_ALERT = 0.2  # алерт, если сдвинулось больше 20% фич
DRIFT_SHARE_RETRAIN = 0.3
AUC_DROP_ALERT = 0.05   # абсолютное падение AUC относительно эталона
AUC_DROP_RETRAIN = 0.10
MISSING_ALERT = 0.05


def psi(ref, cur, bins=10):
    """Population Stability Index по квантильным корзинам эталона."""
    ref, cur = np.asarray(ref, float), np.asarray(cur, float)
    ref, cur = ref[~np.isnan(ref)], cur[~np.isnan(cur)]
    if len(ref) == 0 or len(cur) == 0:
        return np.nan
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1)))
    if len(edges) < 2:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    r = np.histogram(ref, edges)[0] / len(ref) + 1e-6
    c = np.histogram(cur, edges)[0] / len(cur) + 1e-6
    return float(np.sum((c - r) * np.log(c / r)))


def drift_table(ref, cur, features):
    rows = []
    for f in features:
        r, c = ref[f].astype(float), cur[f].astype(float)
        lo, hi = r.quantile(0.001), r.quantile(0.999)
        ks = ks_2samp(r.dropna(), c.dropna()) if r.notna().any() and c.notna().any() else None
        p = psi(r, c)
        rows.append({'feature': f, 'psi': p, 'ks_stat': ks.statistic if ks else np.nan,
                     'ks_pvalue': ks.pvalue if ks else np.nan,
                     'missing_ref': r.isna().mean(), 'missing_cur': c.isna().mean(),
                     'out_of_range_cur': float(((c < lo) | (c > hi)).mean()),
                     'drift': bool(p > PSI_DRIFT)})
    return pd.DataFrame(rows)


def drift_score(table):
    """Доля фич со значимым сдвигом (PSI > 0.2)."""
    return float(table['drift'].mean())


def evidently_drift(ref, cur, features, html_path=None):
    """Отчёт Evidently (DataDriftPreset): доля сдвинутых колонок и HTML-отчёт. None, если Evidently недоступен."""
    try:
        from evidently import Report
        from evidently.presets import DataDriftPreset
    except Exception:
        return None
    snap = Report([DataDriftPreset()]).run(current_data=cur[features], reference_data=ref[features])
    if html_path:
        snap.save_html(html_path)
    d = snap.dict()
    for m in d.get('metrics', []):
        if 'DriftedColumnsCount' in str(m.get('metric_name') or m.get('metric_id') or m.get('config')):
            v = m.get('value', {})
            return {'drifted_columns': v.get('count'), 'share': v.get('share'), 'raw': d}
    return {'drifted_columns': None, 'share': None, 'raw': d}


def send_alert(log_dir, level, kind, message, **ctx):
    """Алерт: запись в журнал; при заданном SLACK_WEBHOOK_URL - сообщение в Slack, при ALERT_EMAIL - письмо."""
    rec = {'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'level': level, 'kind': kind,
           'message': message, **ctx}
    os.makedirs(log_dir, exist_ok=True)
    with open(os.path.join(log_dir, 'alerts.jsonl'), 'a', encoding='utf-8') as fh:
        fh.write(json.dumps(rec, ensure_ascii=False, default=str) + '\n')
    url = os.environ.get('SLACK_WEBHOOK_URL')
    if url:
        try:
            import requests
            requests.post(url, json={'text': f'ML MONITORING ALERT [{kind}]: {message}'}, timeout=5)
        except Exception:
            pass
    if os.environ.get('ALERT_EMAIL') and os.environ.get('SMTP_HOST'):
        try:
            import smtplib
            from email.message import EmailMessage
            msg = EmailMessage()
            msg['Subject'], msg['To'], msg['From'] = f'ML alert: {kind}', os.environ['ALERT_EMAIL'], 'ml-monitoring'
            msg.set_content(message)
            with smtplib.SMTP(os.environ['SMTP_HOST']) as s:
                s.send_message(msg)
        except Exception:
            pass
    return rec


def check_retrain_needed(drift_share, auc_drop, days_since_train, weekly_days=7):
    """Решение о переобучении: сильный дрейф данных, падение качества или плановый еженедельный retrain."""
    reasons = []
    if drift_share > DRIFT_SHARE_RETRAIN:
        reasons.append(f'дрейф: сдвинуто {drift_share:.0%} фич > {DRIFT_SHARE_RETRAIN:.0%}')
    if auc_drop > AUC_DROP_RETRAIN:
        reasons.append(f'AUC упал на {auc_drop:.3f} > {AUC_DROP_RETRAIN}')
    if days_since_train >= weekly_days:
        reasons.append(f'плановое переобучение: {days_since_train} дн. с последнего')
    return bool(reasons), reasons
