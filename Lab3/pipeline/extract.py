"""Extract: чтение часовых партиций из landing-зоны и добавление идемпотентного ключа."""
import glob
import json
import os

import duckdb
import pyarrow as pa

from .common import maybe_fail

# Контракт источника (схема v1): обязательные поля нужны модели, без необязательных можно обойтись
SOURCE_COLUMNS = ['event_time', 'event_type', 'product_id', 'category_id', 'category_code',
                  'brand', 'price', 'user_id', 'user_session']
REQUIRED = ['event_time', 'event_type', 'product_id', 'price', 'user_id', 'user_session']
KEY_COLUMNS = ['event_time', 'event_type', 'product_id', 'user_id', 'user_session']


def list_partitions(paths):
    """Все часовые партиции landing в виде 'YYYY-MM-DD/HH'."""
    parts = []
    for d in glob.glob(os.path.join(paths.landing, 'dt=*', 'hr=*')):
        if glob.glob(os.path.join(d, '*.parquet')):
            dt = os.path.basename(os.path.dirname(d))[3:]
            hr = os.path.basename(d)[3:]
            parts.append(f'{dt}/{hr}')
    return sorted(parts)


def processed_state(paths):
    f = os.path.join(paths.state, 'processed.json')
    if os.path.exists(f):
        with open(f) as fh:
            return json.load(fh)
    return {}


def mark_processed(paths, partitions, status, run_id):
    state = processed_state(paths)
    for p in partitions:
        state[p] = {'status': status, 'run_id': run_id}
    with open(os.path.join(paths.state, 'processed.json'), 'w') as fh:
        json.dump(state, fh, indent=1, sort_keys=True)


def new_partitions(paths, until=None):
    """Партиции, которые ещё не обработаны (аналог сенсора: ждём новые файлы)."""
    done = processed_state(paths)
    return [p for p in list_partitions(paths) if p not in done and (until is None or p < until)]


def partition_files(paths, partition):
    """Партиция 'YYYY-MM-DD/HH' - один час, 'YYYY-MM-DD' - все часы дня."""
    if '/' in partition:
        dt, hr = partition.split('/')
        pattern = os.path.join(paths.landing, f'dt={dt}', f'hr={hr}', '*.parquet')
    else:
        pattern = os.path.join(paths.landing, f'dt={partition}', 'hr=*', '*.parquet')
    return sorted(glob.glob(pattern))


def extract_partition(paths, partition):
    """Читает файлы партиции как есть (все поля строками) и считает idempotency_key."""
    maybe_fail('extract')
    files = partition_files(paths, partition)
    if not files:
        raise FileNotFoundError(f'нет файлов для партиции {partition}')
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='512MB'; SET threads=2")
        src = con.execute('SELECT * FROM read_parquet(?, union_by_name=true, hive_partitioning=false) LIMIT 0',
                          [files]).fetch_arrow_table()
        cols = [c for c in src.column_names]
        casts = ', '.join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in cols)
        present_key = [c for c in KEY_COLUMNS if c in cols]
        key_expr = "md5(concat_ws('|', " + ', '.join(f'coalesce("{c}", \'\')' for c in present_key) + '))'
        tbl = con.execute(
            f'SELECT {casts}, {key_expr} AS idempotency_key, ? AS _partition '
            f'FROM read_parquet(?, union_by_name=true, hive_partitioning=false)',
            [partition, files]).fetch_arrow_table()
    finally:
        con.close()
    return tbl
