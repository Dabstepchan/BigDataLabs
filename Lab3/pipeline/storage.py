"""Load: Delta Lake с UPSERT (MERGE), история версий и time travel."""
import multiprocessing as mp
import os
import resource
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

RAW_SCHEMA = pa.schema([
    ('idempotency_key', pa.string()), ('event_time', pa.timestamp('us')), ('event_date', pa.date32()),
    ('event_hour', pa.int8()), ('event_type', pa.string()), ('product_id', pa.int64()),
    ('category_id', pa.int64()), ('category_code', pa.string()), ('brand', pa.string()), ('price', pa.float64()),
    ('user_id', pa.int64()), ('user_session', pa.string()),
])
DLQ_SCHEMA = pa.schema([
    ('dlq_key', pa.string()), ('idempotency_key', pa.string()), ('reason', pa.string()),
    ('payload', pa.string()), ('_partition', pa.string()), ('run_id', pa.string()), ('detected_at', pa.string()),
])
RECO_SCHEMA = pa.schema([
    ('user_id', pa.int64()), ('rank', pa.int32()), ('product_id', pa.int64()), ('category_id', pa.int64()),
    ('score', pa.float64()), ('source', pa.string()), ('model_name', pa.string()),
    ('model_version', pa.string()), ('generated_at', pa.timestamp('us')), ('run_id', pa.string()),
    ('user_bucket', pa.int8()),
])
RECO_BUCKETS = 8   # корзины user_id % 8: публикация и MERGE идут по корзине, память ограничена её размером

TABLES = {
    'raw': (RAW_SCHEMA, ['event_date', 'event_hour'],
            't.idempotency_key = s.idempotency_key AND t.event_date = s.event_date AND t.event_hour = s.event_hour'),
    'dlq': (DLQ_SCHEMA, None, 't.dlq_key = s.dlq_key'),
    'reco': (RECO_SCHEMA, ['user_bucket'], 't.user_id = s.user_id AND t.rank = s.rank'),
}


def ensure_table(path, schema, partition_by=None):
    if not os.path.exists(os.path.join(path, '_delta_log')):
        DeltaTable.create(path, schema=schema, partition_by=partition_by, mode='ignore')
    return DeltaTable(path)


def _upsert(path, kind, data):
    schema, part, predicate = TABLES[kind]
    dt = ensure_table(path, schema, part)
    data = data.select(schema.names).cast(schema)
    if data.num_rows == 0:
        return {'num_source_rows': 0, 'num_target_rows_inserted': 0, 'num_target_rows_updated': 0,
                'version': dt.version()}
    if part:
        # литералы партиций в условии: delta-rs читает только затронутые партиции, а не всю таблицу
        import pyarrow.compute as pc
        conds = []
        for c in part:
            vals = pc.unique(data.column(c)).to_pylist()
            lit = ', '.join(f"'{v}'" if not isinstance(v, int) else str(v) for v in vals)
            conds.append(f't.{c} IN ({lit})')
        predicate = ' AND '.join(conds) + ' AND ' + predicate
    res = (dt.merge(source=data, predicate=predicate, source_alias='s', target_alias='t')
             .when_matched_update_all().when_not_matched_insert_all().execute())
    res['version'] = DeltaTable(path).version()
    res['worker_peak_mb'] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
    return res


# delta-rs не возвращает память после MERGE (около 30 МБ на коммит), поэтому запись идёт
# в отдельном процессе, который пересоздаётся каждые 4 коммита
_POOL = None


def _pool():
    global _POOL
    if _POOL is None:
        _POOL = ProcessPoolExecutor(max_workers=1, mp_context=mp.get_context('spawn'), max_tasks_per_child=4)
    return _POOL


def shutdown_pool():
    """Останавливает процесс записи (перед тяжёлыми этапами, чтобы не держать его память)."""
    global _POOL
    if _POOL is not None:
        _POOL.shutdown(wait=True)
        _POOL = None


def upsert(path, kind, data):
    """MERGE INTO target USING source ON key WHEN MATCHED UPDATE * WHEN NOT MATCHED INSERT *."""
    global _POOL
    try:
        return _pool().submit(_upsert, path, kind, data).result()
    except BrokenProcessPool:
        _POOL = None
        raise


def _publish_bucket(warehouse, path, bucket, run_id):
    import duckdb
    con = duckdb.connect(warehouse, read_only=True)
    try:
        tbl = con.execute(f"""SELECT *, ? AS run_id, CAST(user_id % {RECO_BUCKETS} AS TINYINT) AS user_bucket
                              FROM user_recommendations WHERE user_id % {RECO_BUCKETS} = ?""",
                          [run_id, bucket]).fetch_arrow_table()
    finally:
        con.close()
    return _upsert(path, 'reco', tbl)


def publish_recommendations(warehouse, path, run_id):
    """MERGE витрины user_recommendations в таблицу сервинга по корзинам пользователей."""
    global _POOL
    total = {'num_source_rows': 0, 'num_target_rows_inserted': 0, 'num_target_rows_updated': 0}
    try:
        for b in range(RECO_BUCKETS):
            r = _pool().submit(_publish_bucket, warehouse, path, b, run_id).result()
            for k in total:
                total[k] += r[k]
            total['version'] = r['version']
    except BrokenProcessPool:
        _POOL = None
        raise
    return total


def insert_only(path, kind, data):
    """Неидемпотентная загрузка (обычный INSERT/append) - только для сравнения с MERGE."""
    schema, part, _ = TABLES[kind]
    ensure_table(path, schema, part)
    write_deltalake(path, data.select(schema.names).cast(schema), mode='append', partition_by=part)
    return DeltaTable(path).version()


def table_version(path):
    return DeltaTable(path).version() if os.path.exists(os.path.join(path, '_delta_log')) else -1


def history(path):
    return DeltaTable(path).history()


def read_version(path, version=None, columns=None):
    """Time travel: чтение таблицы в состоянии на версию version (None - текущая)."""
    dt = DeltaTable(path)
    if version is not None:
        dt.load_as_version(version)
    return dt.to_pyarrow_table(columns=columns)


def restore(path, version):
    """Откат таблицы к версии version (новый коммит RESTORE, история сохраняется)."""
    return DeltaTable(path).restore(version)
