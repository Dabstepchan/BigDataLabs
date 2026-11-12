"""Онлайн-обучение (River): модель учится на каждом событии потока, качество меряется прогрессивно
(сначала предсказание, потом обучение на той же строке), concept drift отслеживает ADWIN."""
import time

import numpy as np
import pandas as pd
from river import drift, linear_model, optim, preprocessing


def make_online(lr=0.01):
    return preprocessing.StandardScaler() | linear_model.LogisticRegression(optimizer=optim.SGD(lr))


def to_records(df, features):
    return df[features].fillna(0.0).astype(float).to_dict('records')


def warm_start(model, df, features):
    """Начальное обучение на истории (один проход), как стартовая точка потока."""
    for x, y in zip(to_records(df, features), df['label'].to_numpy()):
        model.learn_one(x, bool(y))
    return model


def run_stream(model, df, features, deploy_every=None, on_deploy=None):
    """Прогрессивная оценка на потоке: для каждой строки predict -> learn. Возвращает вероятности,
    время обработки и моменты срабатывания ADWIN по ошибке."""
    det = drift.ADWIN(delta=0.002)
    probs, drifts, upd_s = np.empty(len(df)), [], 0.0
    ts = df['event_timestamp'].to_numpy()
    for i, (x, y) in enumerate(zip(to_records(df, features), df['label'].to_numpy())):
        p = model.predict_proba_one(x).get(True, 0.0)
        probs[i] = p
        t = time.perf_counter()
        model.learn_one(x, bool(y))
        upd_s += time.perf_counter() - t
        det.update(abs(float(y) - p))
        if det.drift_detected:
            drifts.append(ts[i])
        if deploy_every and on_deploy and (i + 1) % deploy_every == 0:
            on_deploy(model, i + 1)
    return probs, upd_s, drifts


def predict_batch(model, df, features):
    return np.array([model.predict_proba_one(x).get(True, 0.0) for x in to_records(df, features)])
