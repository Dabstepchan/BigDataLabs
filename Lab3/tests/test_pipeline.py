"""Проверки пайплайна на тестовых данных tests/data/landing (две часовые партиции из 2019-Oct.csv)."""
import os
import shutil
import sys

import duckdb
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
SAMPLE = os.path.join(ROOT, 'tests', 'data', 'landing')


@pytest.fixture
def env(tmp_path, monkeypatch):
    shutil.copytree(SAMPLE, tmp_path / 'landing')
    monkeypatch.setenv('LAB3_ROOT', str(tmp_path))
    monkeypatch.setenv('LAB3_GX_ROOT', str(tmp_path))
    monkeypatch.setenv('LAB3_BACKOFF', '0')
    monkeypatch.chdir(ROOT)
    return tmp_path


def raw_stats(root):
    con = duckdb.connect()
    con.execute('INSTALL delta; LOAD delta')
    n, keys = con.execute(f"SELECT count(*), count(DISTINCT idempotency_key) "
                          f"FROM delta_scan('{root}/lake/raw_events')").fetchone()
    con.close()
    return n, keys


def test_idempotency(env):
    from prefect_flows.etl_pipeline import run_pipeline
    from pipeline import extract as ex
    from pipeline.common import get_paths
    parts = ex.list_partitions(get_paths())
    first = run_pipeline(partitions=parts)
    n1, k1 = raw_stats(env)
    second = run_pipeline(partitions=parts)
    n2, k2 = raw_stats(env)
    assert first['status'] == 'ok' and second['status'] == 'ok'
    assert n2 == n1, 'Пайплайн не идемпотентен: число строк выросло'
    assert k2 == n2, 'В raw_events есть дубликаты idempotency_key'
    assert second['inserted'] == 0


def test_dlq(env):
    from prefect_flows.etl_pipeline import run_pipeline
    from pipeline.common import get_paths
    from pipeline import extract as ex
    paths = get_paths()
    part = ex.list_partitions(paths)[0]
    dt, hr = part.split('/')
    con = duckdb.connect()
    con.execute(f"""
        COPY (SELECT * REPLACE ('abc' AS price) FROM read_parquet('{SAMPLE}/dt={dt}/hr={hr}/*.parquet') LIMIT 10)
        TO '{env}/landing/dt={dt}/hr={hr}/corrupted.parquet' (FORMAT parquet)""")
    con.close()
    res = run_pipeline(partitions=[part], transform=False)
    assert res['rows_dlq'] >= 10, 'DLQ не заполняется'
