"""Пакетное обучение: кандидаты моделей, оценка, логирование в MLflow и Model Registry."""
import time

import mlflow
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, f1_score, log_loss, precision_recall_curve, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

MODEL_NAME = 'RecoRanker'


def make_models(seed=42, n_jobs=2):
    return {
        'logreg': Pipeline([('imputer', SimpleImputer(strategy='constant', fill_value=0)),
                            ('scaler', StandardScaler()),
                            ('classifier', LogisticRegression(max_iter=2000, C=1.0))]),
        'random_forest': Pipeline([('imputer', SimpleImputer(strategy='constant', fill_value=0)),
                                   ('classifier', RandomForestClassifier(
                                       n_estimators=100, max_depth=10, min_samples_leaf=20,
                                       n_jobs=n_jobs, random_state=seed))]),
        'hist_gradient_boosting': HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05,
                                                                 random_state=seed),
        'lightgbm': LGBMClassifier(n_estimators=400, learning_rate=0.03, num_leaves=31, min_child_samples=50,
                                   subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
                                   random_state=seed, n_jobs=n_jobs, verbose=-1),
    }


def best_f1_threshold(y, p):
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec * rec / np.clip(prec + rec, 1e-12, None)
    i = int(np.nanargmax(f1[:-1]))
    return float(thr[i])


def evaluate(model, X, y, threshold):
    p = model.predict_proba(X)[:, 1]
    return {'auc': roc_auc_score(y, p), 'pr_auc': average_precision_score(y, p),
            'log_loss': log_loss(y, np.clip(p, 1e-6, 1 - 1e-6)), 'f1': f1_score(y, p >= threshold),
            'positive_rate_pred': float((p >= threshold).mean())}


def latency_ms(model, X, n=300, seed=42):
    """Задержка предсказания одной строки (как в онлайн-инференсе): p50, p95, p99 в мс."""
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(X), n)
    lat = []
    for i in idx:
        row = X.iloc[[i]]
        t = time.perf_counter()
        model.predict_proba(row)
        lat.append((time.perf_counter() - t) * 1000)
    return {'p50_ms': float(np.percentile(lat, 50)), 'p95_ms': float(np.percentile(lat, 95)),
            'p99_ms': float(np.percentile(lat, 99))}


def train_and_log(name, model, train, val, test, features, experiment, tags=None, extra_params=None):
    """Обучение с логированием параметров, метрик и модели в MLflow. Порог F1 выбирается на валидации."""
    mlflow.set_experiment(experiment)
    with mlflow.start_run(run_name=name, tags=tags or {}) as run:
        params = {k: v for k, v in (model.get_params() if hasattr(model, 'get_params') else {}).items()
                  if isinstance(v, (int, float, str, bool)) and len(str(v)) < 100}
        mlflow.log_params({**{k[:250]: v for k, v in list(params.items())[:80]}, **(extra_params or {}),
                           'n_features': len(features), 'n_train': len(train)})
        t = time.perf_counter()
        model.fit(train[features], train['label'])
        fit_s = time.perf_counter() - t
        thr = best_f1_threshold(val['label'], model.predict_proba(val[features])[:, 1])
        m = evaluate(model, test[features], test['label'], thr)
        lat = latency_ms(model, test[features])
        mlflow.log_metrics({**m, **lat, 'fit_seconds': fit_s, 'threshold': thr})
        mlflow.log_dict({'features': features}, 'features.json')
        sig = infer_signature(test[features].head(100), model.predict_proba(test[features].head(100))[:, 1])
        mlflow.sklearn.log_model(model, name='model', signature=sig, input_example=test[features].head(3),
                                 serialization_format='cloudpickle')
    return {'model': name, **m, **lat, 'fit_seconds': fit_s, 'threshold': thr, 'run_id': run.info.run_id}


def register(run_id, alias=None, stage=None, description=''):
    """Регистрация модели из запуска в Model Registry, назначение алиаса и стадии."""
    mv = mlflow.register_model(f'runs:/{run_id}/model', MODEL_NAME)
    client = MlflowClient()
    if description:
        client.update_model_version(MODEL_NAME, mv.version, description=description)
    if alias:
        client.set_registered_model_alias(MODEL_NAME, alias, mv.version)
    if stage:
        try:
            client.transition_model_version_stage(MODEL_NAME, mv.version, stage, archive_existing_versions=True)
        except Exception:
            pass
    return mv.version


def validate_model(model, X, y, min_auc, max_latency_ms=50.0):
    """Проверка перед деплоем: качество не ниже порога и задержка p99 одной строки меньше max_latency_ms."""
    auc = roc_auc_score(y, model.predict_proba(X)[:, 1])
    lat = latency_ms(model, X)
    problems = []
    if auc < min_auc:
        problems.append(f'AUC={auc:.4f} < {min_auc:.4f}')
    if lat['p99_ms'] > max_latency_ms:
        problems.append(f'p99 задержки {lat["p99_ms"]:.2f} мс > {max_latency_ms} мс')
    return {'auc': auc, **lat, 'passed': not problems, 'problems': problems}
