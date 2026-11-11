"""Validate: контроль схемы (schema drift), построчные правила (DLQ) и проверки Great Expectations."""
import json
import logging
import os

import duckdb
import pandas as pd
import pyarrow as pa

from .common import now_utc, send_alert
from .extract import REQUIRED, SOURCE_COLUMNS

os.environ.setdefault('GX_ANALYTICS_ENABLED', 'False')
logging.getLogger('great_expectations').setLevel(logging.ERROR)

EVENT_TYPES = ['view', 'cart', 'purchase', 'remove_from_cart']


class SchemaDriftError(Exception):
    """Схема партиции несовместима с зарегистрированной: нет обязательных полей."""


class DataValidationError(Exception):
    """Пакет не прошёл проверки качества."""


# ---------- реестр схем и schema drift ----------

def schema_registry(paths):
    f = os.path.join(paths.state, 'schema_registry.json')
    if os.path.exists(f):
        with open(f) as fh:
            return json.load(fh)
    reg = {'active': 1, 'versions': {'1': {'columns': SOURCE_COLUMNS, 'required': REQUIRED,
                                           'registered': now_utc()}}, 'candidates': []}
    with open(f, 'w') as fh:
        json.dump(reg, fh, indent=1)
    return reg


def _save_registry(paths, reg):
    with open(os.path.join(paths.state, 'schema_registry.json'), 'w') as fh:
        json.dump(reg, fh, indent=1)


def gx_columns_check(columns, expected):
    """Проверка набора колонок через Great Expectations (ExpectTableColumnsToMatchSet)."""
    import great_expectations as gx
    ctx = gx_context()
    ds = ctx.data_sources.add_or_update_pandas('pipeline') if 'pipeline' not in ctx.data_sources.all() \
        else ctx.data_sources.get('pipeline')
    try:
        bd = ds.get_asset('schema').get_batch_definition('whole')
    except Exception:
        bd = ds.add_dataframe_asset('schema').add_batch_definition_whole_dataframe('whole')
    df = pd.DataFrame({c: pd.Series([], dtype='object') for c in columns})
    res = bd.get_batch(batch_parameters={'dataframe': df}).validate(
        gx.expectations.ExpectTableColumnsToMatchSet(column_set=expected, exact_match=True))
    mism = (res.result.get('details') or {}).get('mismatched') or {}
    return res.success, sorted(mism.get('unexpected', [])), sorted(mism.get('missing', []))


def check_schema(paths, table, run_id):
    """Сверяет схему партиции с активной версией реестра и приводит пакет к ней (fallback на прежнюю схему).

    Лишние колонки отбрасываются, недостающие необязательные заполняются NULL, о том и другом
    отправляется алерт. Нет обязательной колонки - SchemaDriftError, партиция уходит в DLQ целиком.
    """
    reg = schema_registry(paths)
    ver = reg['versions'][str(reg['active'])]
    data_cols = [c for c in table.column_names if c not in ('idempotency_key', '_partition')]
    ok, added, missing = gx_columns_check(data_cols, ver['columns'])
    report = {'schema_version': reg['active'], 'ok': ok, 'added': added, 'missing': missing}
    if ok:
        return table, report
    part = table.column('_partition')[0].as_py() if table.num_rows else '?'
    missing_req = [c for c in missing if c in ver['required']]
    if added and not any(c['columns'] == data_cols for c in reg['candidates']):
        reg['candidates'].append({'columns': data_cols, 'seen': now_utc(), 'partition': part})
        _save_registry(paths, reg)
    if missing_req:
        send_alert(paths, 'critical', 'schema_drift',
                   f'партиция {part}: нет обязательных колонок {missing_req}, лишние {added}; партиция отправлена в DLQ',
                   run_id=run_id, partition=part)
        raise SchemaDriftError(f'нет обязательных колонок {missing_req}')
    send_alert(paths, 'warning', 'schema_drift',
               f'партиция {part}: лишние колонки {added} отброшены, недостающие {missing} заполнены NULL '
               f'(применена схема v{reg["active"]})', run_id=run_id, partition=part)
    cols = []
    for c in ver['columns']:
        cols.append(table.column(c) if c in table.column_names else pa.nulls(table.num_rows, pa.string()))
    cols += [table.column('idempotency_key'), table.column('_partition')]
    return pa.table(cols, names=ver['columns'] + ['idempotency_key', '_partition']), report


def drift_to_dlq(table, reason, run_id):
    """Вся партиция с несовместимой схемой превращается в записи DLQ (исходная строка в payload)."""
    con = duckdb.connect()
    try:
        con.register('b', table)
        return con.execute(f"""
            SELECT md5(idempotency_key || '|' || ?) AS dlq_key, idempotency_key, ? AS reason,
                   to_json(b)::VARCHAR AS payload,
                   _partition, ? AS run_id, ? AS detected_at
            FROM b""", [reason, reason, run_id, now_utc()]).fetch_arrow_table()
    finally:
        con.close()


# ---------- построчные правила ----------

ROW_RULES = """
    CASE WHEN ts IS NULL THEN 'bad_event_time'
         WHEN event_type IS NULL OR event_type NOT IN ('view', 'cart', 'purchase', 'remove_from_cart')
              THEN 'bad_event_type'
         WHEN pid IS NULL OR pid <= 0 THEN 'bad_product_id'
         WHEN uid IS NULL OR uid <= 0 THEN 'bad_user_id'
         WHEN pr IS NULL THEN 'bad_price'
         WHEN pr <= 0 THEN 'price_not_positive'
         WHEN user_session IS NULL OR user_session = '' THEN 'no_session'
    END"""


def split_rows(table, run_id):
    """Делит пакет на валидные строки (типизированные, без дублей) и записи для DLQ."""
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='768MB'; SET threads=2")
        con.register('b', table)
        con.execute(f"""
            CREATE TEMP TABLE c AS
            SELECT *, {ROW_RULES} AS reason FROM (
                SELECT *, TRY_STRPTIME(event_time, '%Y-%m-%d %H:%M:%S UTC') AS ts,
                       TRY_CAST(product_id AS BIGINT) AS pid, TRY_CAST(category_id AS BIGINT) AS cid,
                       TRY_CAST(price AS DOUBLE) AS pr, TRY_CAST(user_id AS BIGINT) AS uid
                FROM b)""")
        con.unregister('b')
        rejected = con.execute("""
            SELECT md5(idempotency_key || '|' || reason) AS dlq_key, idempotency_key, reason,
                   to_json({'event_time': event_time, 'event_type': event_type, 'product_id': product_id,
                            'category_id': category_id, 'category_code': category_code, 'brand': brand,
                            'price': price, 'user_id': user_id, 'user_session': user_session})::VARCHAR AS payload,
                   _partition, ? AS run_id, ? AS detected_at
            FROM c WHERE reason IS NOT NULL""", [run_id, now_utc()]).fetch_arrow_table()
        n_valid = con.execute('SELECT count(*) FROM c WHERE reason IS NULL').fetchone()[0]
        valid = con.execute("""
            SELECT idempotency_key, ts AS event_time, CAST(ts AS DATE) AS event_date,
                   CAST(hour(ts) AS TINYINT) AS event_hour, event_type,
                   pid AS product_id, cid AS category_id, category_code, brand, pr AS price,
                   uid AS user_id, user_session
            FROM c WHERE reason IS NULL
            QUALIFY row_number() OVER (PARTITION BY idempotency_key ORDER BY _partition) = 1
            ORDER BY event_time""").fetch_arrow_table()
    finally:
        con.close()
    return valid, rejected, {'rows_valid_before_dedup': n_valid, 'duplicates_in_batch': n_valid - valid.num_rows}


# ---------- Great Expectations: наборы проверок, чекпоинты, Data Docs ----------

def _set_no_progress(ctx):
    try:
        from great_expectations.data_context.types.base import ProgressBarsConfig
        ctx.variables.progress_bars = ProgressBarsConfig(globally=False)
    except Exception:
        pass


def gx_context(gx_root=None):
    import great_expectations as gx
    root = os.path.abspath(gx_root or os.environ.get('LAB3_GX_ROOT', '.'))
    ctx = gx.get_context(mode='file', project_root_dir=root)
    _set_no_progress(ctx)
    return ctx


def suite_definitions(k=10, model_version='1.0', min_price=0.0, max_price=100_000.0):
    """Наборы ожиданий: до загрузки (raw_batch) и после трансформации (recommendations, fact_sales)."""
    import great_expectations as gx
    E = gx.expectations
    return {
        'raw_batch': [
            *[E.ExpectColumnValuesToNotBeNull(column=c) for c in
              ('idempotency_key', 'event_time', 'event_type', 'product_id', 'user_id', 'price', 'user_session')],
            E.ExpectColumnValuesToBeInSet(column='event_type', value_set=EVENT_TYPES),
            E.ExpectColumnValuesToBeBetween(column='price', min_value=min_price, max_value=max_price,
                                            strict_min=True),
            E.ExpectColumnValuesToBeBetween(column='user_id', min_value=1),
            E.ExpectColumnValuesToBeUnique(column='idempotency_key'),
        ],
        'recommendations': [
            *[E.ExpectColumnValuesToNotBeNull(column=c) for c in ('user_id', 'rank', 'product_id', 'model_version')],
            E.ExpectColumnValuesToBeBetween(column='rank', min_value=1, max_value=k),
            E.ExpectCompoundColumnsToBeUnique(column_list=['user_id', 'rank']),
            E.ExpectCompoundColumnsToBeUnique(column_list=['user_id', 'product_id']),
            E.ExpectColumnValuesToBeInSet(column='model_version', value_set=[model_version]),
            # связь между таблицами: товар рекомендации есть в dim_products
            E.ExpectColumnValuesToBeInSet(column='product_in_dim', value_set=[True]),
        ],
        'fact_sales': [
            E.ExpectColumnValuesToNotBeNull(column='sale_id'),
            E.ExpectColumnValuesToBeUnique(column='sale_id'),
            E.ExpectColumnValuesToBeBetween(column='amount_usd', min_value=0, strict_min=True),
            E.ExpectColumnValuesToNotBeNull(column='amount_rub'),
            # связи между таблицами: fact_sales -> dim_products, fact_sales -> dim_users
            E.ExpectColumnValuesToBeInSet(column='product_in_dim', value_set=[True]),
            E.ExpectColumnValuesToBeInSet(column='user_in_dim', value_set=[True]),
        ],
    }


def ensure_gx_objects(ctx, **suite_kwargs):
    """Создаёт (или обновляет) источник, наборы, определения валидаций и чекпоинты в контексте GX."""
    import great_expectations as gx
    ds = ctx.data_sources.get('pipeline') if 'pipeline' in ctx.data_sources.all() \
        else ctx.data_sources.add_pandas('pipeline')
    for name, exps in suite_definitions(**suite_kwargs).items():
        try:
            asset = ds.get_asset(name)
        except Exception:
            asset = ds.add_dataframe_asset(name)
        try:
            bd = asset.get_batch_definition('whole')
        except Exception:
            bd = asset.add_batch_definition_whole_dataframe('whole')
        suite = gx.ExpectationSuite(name=name, expectations=exps)
        suite = ctx.suites.add_or_update(suite)
        vd = ctx.validation_definitions.add_or_update(gx.ValidationDefinition(name=f'{name}_vd', data=bd, suite=suite))
        # Data Docs пересобираются целиком на каждом чекпоинте, поэтому обновляются только проверками витрин
        # (раз за запуск), а не проверкой каждой партиции
        actions = [] if name == 'raw_batch' else [gx.checkpoint.UpdateDataDocsAction(name='update_docs')]
        ctx.checkpoints.add_or_update(gx.Checkpoint(
            name=f'{name}_checkpoint', validation_definitions=[vd], actions=actions,
            result_format={'result_format': 'SUMMARY', 'partial_unexpected_count': 5}))


def run_checkpoint(ctx, name, df):
    """Запускает чекпоинт набора name на DataFrame и возвращает краткий отчёт."""
    res = ctx.checkpoints.get(f'{name}_checkpoint').run(batch_parameters={'dataframe': df})
    vr = next(iter(res.run_results.values()))
    failed = []
    for r in vr.results:
        if not r.success:
            cfg = r.expectation_config
            kw = cfg.kwargs
            failed.append({'expectation': cfg.type, 'column': kw.get('column') or kw.get('column_list'),
                           'unexpected_count': r.result.get('unexpected_count'),
                           'unexpected_percent': r.result.get('unexpected_percent'),
                           'examples': r.result.get('partial_unexpected_list')})
    st = vr.statistics
    return {'suite': name, 'success': bool(res.success), 'evaluated': st['evaluated_expectations'],
            'successful': st['successful_expectations'], 'success_percent': st['success_percent'],
            'failed': failed}
