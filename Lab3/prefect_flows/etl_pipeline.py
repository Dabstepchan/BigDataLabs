"""Оркестрация ELT-пайплайна рекомендаций в Prefect.

Граф: detect -> [ingest_partition: extract -> validate -> load (MERGE raw + DLQ)] x N партиций
      -> transform (dbt build) -> quality_gate (GX) -> publish (MERGE рекомендаций).
При сбое transform/quality_gate/publish в сервинге остаются предыдущие рекомендации (fallback).
"""
import json
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

import pyarrow as pa
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.context import get_run_context
from prefect.tasks import exponential_backoff

from pipeline import common
from pipeline import extract as ex
from pipeline import quality as q
from pipeline import storage as st
from pipeline import transform as tr
from pipeline.common import Timer, get_logger, get_paths, log_metric, send_alert

# В production задержки в минутах (backoff_factor=60); в работе секунды, чтобы проверка не ждала
BACKOFF = float(os.environ.get('LAB3_BACKOFF', '1'))
RETRY = dict(retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=BACKOFF),
             retry_jitter_factor=0.2, cache_policy=NO_CACHE)


def _log(msg):
    try:
        get_run_logger().info(msg)
    except Exception:
        pass
    get_logger(get_paths()).info(msg)


@task(name='detect_partitions', cache_policy=NO_CACHE)
def detect_partitions(until=None):
    paths = get_paths()
    parts = ex.new_partitions(paths, until)
    pending = [p for p, s in ex.processed_state(paths).items() if s['status'] == 'loaded']
    _log(f'новых партиций: {len(parts)}, ждут трансформации: {len(pending)}')
    return parts, pending


@task(name='extract', **RETRY)
def extract_task(partition, run_id):
    paths = get_paths()
    with Timer() as t:
        tbl = ex.extract_partition(paths, partition)
    log_metric(paths, run_id, 'extract', partition=partition, rows=tbl.num_rows, seconds=round(t.seconds, 3))
    _log(f'extract {partition}: {tbl.num_rows} строк за {t.seconds:.2f} с')
    return tbl


@task(name='validate', cache_policy=NO_CACHE)
def validate_task(tbl, partition, run_id):
    """Схема -> построчные правила (DLQ) -> Great Expectations перед загрузкой."""
    paths = get_paths()
    with Timer() as t:
        try:
            tbl, schema_report = q.check_schema(paths, tbl, run_id)
        except q.SchemaDriftError as e:
            rejected = q.drift_to_dlq(tbl, f'schema_drift: {e}', run_id)
            log_metric(paths, run_id, 'validate', partition=partition, rows_in=tbl.num_rows, rows_valid=0,
                       rows_dlq=rejected.num_rows, schema_drift=1)
            return {'partition': partition, 'valid': None, 'rejected': rejected,
                    'report': {'rows_in': tbl.num_rows, 'rows_valid': 0, 'rows_dlq': rejected.num_rows,
                               'schema': 'drift_quarantined', 'gx': None}}
        valid, rejected, rep = q.split_rows(tbl, run_id)
        gx_rep = None
        if valid.num_rows:
            ctx = q.gx_context()
            cols = ['idempotency_key', 'event_time', 'event_type', 'product_id', 'user_id', 'price', 'user_session']
            gx_rep = q.run_checkpoint(ctx, 'raw_batch', valid.select(cols).to_pandas())
            if not gx_rep['success']:
                send_alert(paths, 'critical', 'gx_failed', f'партиция {partition}: проверки GX не пройдены '
                           f'({gx_rep["failed"]}), партиция отправлена в DLQ', run_id=run_id)
                bad = q.drift_to_dlq(tbl, 'gx_failed', run_id)
                rejected = pa.concat_tables([rejected, bad.select(rejected.column_names)])
                valid = None
    n_dlq = rejected.num_rows
    share = n_dlq / max(tbl.num_rows, 1)
    report = {'rows_in': tbl.num_rows, 'rows_valid': valid.num_rows if valid is not None else 0,
              'rows_dlq': n_dlq, 'dlq_share': share, 'duplicates_in_batch': rep['duplicates_in_batch'],
              'schema': 'ok' if schema_report['ok'] else 'drift_fallback', 'gx': gx_rep,
              'seconds': round(t.seconds, 3)}
    log_metric(paths, run_id, 'validate', partition=partition, rows_in=tbl.num_rows, rows_valid=report['rows_valid'],
               rows_dlq=n_dlq, duplicates=rep['duplicates_in_batch'], seconds=round(t.seconds, 3),
               gx_success_percent=gx_rep['success_percent'] if gx_rep else None)
    if share > common.DLQ_ALERT_SHARE:
        send_alert(paths, 'warning', 'dlq_share', f'партиция {partition}: в DLQ {n_dlq} строк ({share:.2%}) '
                   f'при пороге {common.DLQ_ALERT_SHARE:.0%}', run_id=run_id)
    return {'partition': partition, 'valid': valid, 'rejected': rejected, 'report': report}


@flow(name='ingest_partition', flow_run_name='ingest-{partition}')
def ingest_partition(partition, run_id):
    """Подпоток для повторяющегося шаблона (аналог SubDAG): extract -> validate -> load одной партиции.
    Наружу отдаётся только отчёт, таблицы освобождаются сразу после загрузки."""
    tbl = extract_task(partition, run_id)
    res = validate_task(tbl, partition, run_id)
    del tbl
    loaded = load_task(res, run_id)
    return {'partition': partition, 'report': res['report'], 'load': loaded}


@task(name='load', **RETRY)
def load_task(res, run_id):
    """MERGE валидных строк партиции в raw_events, затем MERGE отклонённых в DLQ. Оба шага идемпотентны."""
    paths = get_paths()
    with Timer() as t:
        if res['valid'] is not None and res['valid'].num_rows:
            raw = st.upsert(paths.raw, 'raw', res['valid'])
        else:
            raw = {'num_source_rows': 0, 'num_target_rows_inserted': 0, 'num_target_rows_updated': 0,
                   'version': st.table_version(paths.raw)}
        common.maybe_fail('load')   # точка сбоя: raw уже записан, DLQ ещё нет
        if res['rejected'].num_rows:
            dlq = st.upsert(paths.dlq, 'dlq', res['rejected'])
        else:
            dlq = {'num_target_rows_inserted': 0, 'version': st.table_version(paths.dlq)}
    log_metric(paths, run_id, 'load', partition=res['partition'], rows=raw['num_source_rows'],
               inserted=raw['num_target_rows_inserted'], updated=raw['num_target_rows_updated'],
               dlq_inserted=dlq['num_target_rows_inserted'], raw_version=raw['version'], seconds=round(t.seconds, 3))
    _log(f'load {res["partition"]}: raw +{raw["num_target_rows_inserted"]} / обновлено {raw["num_target_rows_updated"]}, '
         f'DLQ +{dlq["num_target_rows_inserted"]}, версия raw {raw["version"]}')
    return {'inserted': raw['num_target_rows_inserted'], 'updated': raw['num_target_rows_updated'],
            'dlq_inserted': dlq['num_target_rows_inserted'], 'raw_version': raw['version']}


@task(name='transform', retries=1, retry_delay_seconds=BACKOFF, cache_policy=NO_CACHE)
def transform_task(as_of, batch_start, run_id):
    paths = get_paths()
    with Timer() as t:
        res = tr.run_dbt(paths, ['build'], tr.model_vars(paths, as_of, batch_start))
    with open(os.path.join(paths.state, 'last_transform.json'), 'w') as fh:
        json.dump({'as_of': as_of, 'batch_start': batch_start, 'run_id': run_id}, fh)
    n_tests = sum(r['node'].startswith('test.') for r in res['results'])
    log_metric(paths, run_id, 'transform', seconds=round(t.seconds, 3), dbt_nodes=len(res['results']),
               dbt_tests=n_tests)
    _log(f'transform: dbt build, {len(res["results"])} узлов ({n_tests} тестов) за {t.seconds:.1f} с')
    return res['results']


@task(name='quality_gate', cache_policy=NO_CACHE)
def quality_gate_task(run_id):
    """Great Expectations после трансформации: рекомендации и продажи, включая связи между таблицами."""
    paths = get_paths()
    with Timer() as t:
        ctx = q.gx_context()
        reco = tr.query(paths, """
            SELECT r.user_id, r.rank, r.product_id, r.model_version, p.product_id IS NOT NULL AS product_in_dim
            FROM user_recommendations r LEFT JOIN dim_products p USING (product_id)""").to_pandas()
        sales = tr.query(paths, """
            SELECT s.sale_id, s.amount_usd, s.amount_rub, p.product_id IS NOT NULL AS product_in_dim,
                   u.user_id IS NOT NULL AS user_in_dim
            FROM fact_sales s LEFT JOIN dim_products p USING (product_id) LEFT JOIN dim_users u USING (user_id)
            WHERE s.event_date >= (SELECT max(event_date) - 1 FROM fact_sales)""").to_pandas()
        reps = [q.run_checkpoint(ctx, 'recommendations', reco), q.run_checkpoint(ctx, 'fact_sales', sales)]
        del reco, sales
        common.release_memory()
    for r in reps:
        log_metric(paths, run_id, f'gx_{r["suite"]}', success=int(r['success']),
                   success_percent=r['success_percent'], seconds=round(t.seconds, 3))
    bad = [r for r in reps if not r['success']]
    if bad:
        raise q.DataValidationError('; '.join(f'{r["suite"]}: {r["failed"]}' for r in bad))
    return reps


@task(name='publish', **RETRY)
def publish_task(run_id):
    """Публикация рекомендаций в сервинг (MERGE по user_id, rank) после проверки согласованности с моделью."""
    paths = get_paths()
    model = tr.production_model(paths)
    versions = set(tr.query(paths, 'SELECT DISTINCT model_version FROM user_recommendations')
                   .column('model_version').to_pylist())
    if versions and versions != {model['model_version']}:
        raise q.DataValidationError(f'витрина построена моделью {versions}, в production {model["model_version"]}')
    with Timer() as t:
        res = st.publish_recommendations(paths.warehouse, paths.reco, run_id)
    st.shutdown_pool()
    log_metric(paths, run_id, 'publish', rows=res['num_source_rows'], inserted=res['num_target_rows_inserted'],
               updated=res['num_target_rows_updated'], reco_version=res['version'], seconds=round(t.seconds, 3))
    _log(f'publish: {res["num_source_rows"]} рекомендаций, версия таблицы {res["version"]}')
    return res


def _on_failure(flow, flow_run, state):
    paths = get_paths()
    send_alert(paths, 'critical', 'flow_failed',
               f'запуск {flow_run.name} упал: {state.message}; в сервинге остаются рекомендации версии '
               f'{st.table_version(paths.reco)}', run_id=str(flow_run.id))


def _bounds(partitions):
    starts, ends = [], []
    for p in partitions:
        if '/' in p:
            d, h = p.split('/')
            s = datetime.strptime(f'{d} {h}', '%Y-%m-%d %H')
            e = s + timedelta(hours=1)
        else:
            s = datetime.strptime(p, '%Y-%m-%d')
            e = s + timedelta(days=1)
        starts.append(s)
        ends.append(e)
    return min(starts), max(ends)


@flow(name='reco_etl', on_failure=[_on_failure], log_prints=True)
def reco_etl(partitions: Optional[list] = None, until: Optional[str] = None, transform: bool = True,
             publish: bool = True, faults: Optional[dict] = None, sla_seconds: float = common.SLA_SECONDS):
    """Один запуск пайплайна: партиции заданы явно (backfill) или находятся автоматически (ежечасный режим)."""
    t0 = time.perf_counter()
    paths = get_paths()
    common.FAULTS.clear()
    common.FAULTS.update(faults or {})
    model = tr.production_model(paths)
    q.ensure_gx_objects(q.gx_context(), k=model['k'], model_version=model['model_version'])
    try:
        run_id = str(get_run_context().flow_run.id)
    except Exception:
        run_id = uuid.uuid4().hex
    pending = []
    if partitions is None:
        partitions, pending = detect_partitions(until)
    if not partitions and not pending:
        return {'run_id': run_id, 'status': 'no_new_data', 'partitions': []}

    results = [ingest_partition(p, run_id) for p in partitions]       # динамическое число подпотоков
    ex.mark_processed(paths, partitions, 'loaded', run_id)
    st.shutdown_pool()
    common.release_memory()
    dlq_total = st.read_version(paths.dlq, columns=['dlq_key']).num_rows if st.table_version(paths.dlq) >= 0 else 0
    if dlq_total > common.DLQ_ALERT_TOTAL:
        send_alert(paths, 'warning', 'dlq_accumulated', f'накоплено {dlq_total} ошибок в DLQ '
                   f'(порог {common.DLQ_ALERT_TOTAL})', run_id=run_id)

    summary = {'run_id': run_id, 'partitions': partitions,
               'rows_in': sum(r['report']['rows_in'] for r in results),
               'rows_valid': sum(r['report']['rows_valid'] for r in results),
               'rows_dlq': sum(r['report']['rows_dlq'] for r in results),
               'duplicates_in_batch': sum(r['report'].get('duplicates_in_batch', 0) for r in results),
               'schema': sorted({r['report']['schema'] for r in results}),
               'inserted': sum(r['load']['inserted'] for r in results),
               'updated': sum(r['load']['updated'] for r in results),
               'dlq_total': dlq_total, 'raw_version': st.table_version(paths.raw),
               'gx_raw_percent': [r['report']['gx']['success_percent'] for r in results if r['report']['gx']],
               'status': 'loaded'}
    if transform:
        batch_start, as_of = _bounds(partitions + pending)
        try:
            transform_task(as_of.strftime('%Y-%m-%d %H:%M:%S'), batch_start.strftime('%Y-%m-%d %H:%M:%S'), run_id)
            gx_reports = quality_gate_task(run_id)
            summary['gx_marts_percent'] = [r['success_percent'] for r in gx_reports]
            if publish:
                pub = publish_task(run_id)
                summary['reco_version'] = pub['version']
            ex.mark_processed(paths, partitions + pending, 'done', run_id)
            summary['status'] = 'ok'
        except Exception as e:
            summary['status'] = 'fallback'
            summary['error'] = f'{type(e).__name__}: {str(e)[:300]}'
            send_alert(paths, 'critical', 'fallback',
                       f'рекомендации не обновлены ({type(e).__name__}); в сервинге остаётся версия '
                       f'{st.table_version(paths.reco)}', run_id=run_id)
    summary['reco_version'] = summary.get('reco_version', st.table_version(paths.reco))
    summary['duration_sec'] = round(time.perf_counter() - t0, 2)
    log_metric(paths, run_id, 'pipeline', seconds=summary['duration_sec'], rows_in=summary['rows_in'],
               rows_dlq=summary['rows_dlq'], status=summary['status'])
    if summary['duration_sec'] > sla_seconds:
        send_alert(paths, 'warning', 'sla', f'запуск занял {summary["duration_sec"]:.1f} с при SLA {sla_seconds:.0f} с',
                   run_id=run_id)
    common.FAULTS.clear()
    return summary


def run_pipeline(root=None, **kwargs):
    """Запуск пайплайна для тестов и CI: root - каталог с landing и lake."""
    if root:
        os.environ['LAB3_ROOT'] = os.path.abspath(root)
    return reco_etl(**kwargs)
