"""Детектор дрейфа: на одинаковых распределениях молчит, на сдвинутых срабатывает."""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from monitoring.drift_detection import PSI_DRIFT, check_retrain_needed, drift_score, drift_table, psi


def test_psi_same_distribution_is_small():
    rng = np.random.default_rng(42)
    assert psi(rng.normal(size=20000), rng.normal(size=20000)) < 0.02


def test_shift_is_detected():
    rng = np.random.default_rng(42)
    ref = pd.DataFrame({'a': rng.normal(size=10000), 'b': rng.exponential(size=10000)})
    cur = pd.DataFrame({'a': rng.normal(1.0, 1, size=10000), 'b': rng.exponential(size=10000)})
    t = drift_table(ref, cur, ['a', 'b'])
    assert t.set_index('feature').loc['a', 'psi'] > PSI_DRIFT
    assert drift_score(t) == 0.5


def test_retrain_logic():
    assert check_retrain_needed(0.5, 0.0, 1)[0]
    assert check_retrain_needed(0.0, 0.2, 1)[0]
    assert not check_retrain_needed(0.1, 0.01, 2)[0]
