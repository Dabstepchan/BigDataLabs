"""Feature Store отдаёт те же фичи, что прямой расчёт по сырым событиям (offline и online)."""
import os
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
REPO = os.path.join(ROOT, 'feature_repo')
DATA = os.path.join(ROOT, 'work', 'data')

pytestmark = pytest.mark.skipif(not os.path.exists(os.path.join(REPO, 'data', 'materialized.json')),
                                reason='Feature Store ещё не создан ноутбуком')


def test_online_matches_direct():
    from feast import FeatureStore
    from training.data import USER_FEATURES, user_features_direct
    store = FeatureStore(repo_path=REPO)
    snap = pd.read_parquet(os.path.join(REPO, 'data', 'user_stats.parquet'))
    with open(os.path.join(REPO, 'data', 'materialized.json')) as fh:      # какой снимок загружен в online store
        last = pd.Timestamp(__import__('json').load(fh)['snapshot'])
    users = snap[snap['event_timestamp'] == last]['user_id'].sample(20, random_state=42).tolist()
    online = pd.DataFrame(store.get_online_features(
        features=[f'user_stats_7d:{c}' for c in USER_FEATURES],
        entity_rows=[{'user_id': int(u)} for u in users]).to_dict())
    for _, row in online.iterrows():
        direct = user_features_direct(DATA, row['user_id'], last)
        assert np.allclose([row[c] for c in USER_FEATURES], [direct[c] for c in USER_FEATURES]), 'Feature mismatch!'
