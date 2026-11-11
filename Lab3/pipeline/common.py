"""Общие части пайплайна: пути, логирование, метрики, алерты, внедрение сбоев."""
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone

SLA_SECONDS = 45 * 60          # задержка обработки по сценарию: < 45 минут
DLQ_ALERT_SHARE = 0.01         # алерт, если в DLQ ушло больше 1% строк пакета (обоснование в ноутбуке)
DLQ_ALERT_TOTAL = 50_000       # алерт, если DLQ накопила больше записей, чем можно разобрать вручную


@dataclass(frozen=True)
class Paths:
    root: str

    def p(self, *parts):
        return os.path.join(self.root, *parts)

    @property
    def landing(self): return self.p('landing')
    @property
    def raw(self): return self.p('lake', 'raw_events')
    @property
    def dlq(self): return self.p('lake', 'dlq')
    @property
    def reco(self): return self.p('lake', 'recommendations')
    @property
    def warehouse(self): return self.p('warehouse.duckdb')
    @property
    def logs(self): return self.p('logs')
    @property
    def state(self): return self.p('state')


def get_paths(root=None):
    paths = Paths(os.path.abspath(root or os.environ.get('LAB3_ROOT', 'work')))
    for d in (paths.logs, paths.state, paths.p('lake')):
        os.makedirs(d, exist_ok=True)
    return paths


def get_logger(paths):
    """Логгер пишет и в консоль, и в файл logs/pipeline.log (один файл на все запуски)."""
    logger = logging.getLogger('etl_pipeline')
    log_file = os.path.join(paths.logs, 'pipeline.log')
    if not any(getattr(h, 'baseFilename', None) == log_file for h in logger.handlers):
        for h in list(logger.handlers):
            if isinstance(h, logging.FileHandler):
                logger.removeHandler(h)
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
        logger.addHandler(fh)
    logger.setLevel(logging.INFO)
    logger.propagate = False      # журнал только в файл, алерты дополнительно в alerts.jsonl
    return logger


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _append_jsonl(path, record):
    with open(path, 'a', encoding='utf-8') as fh:
        fh.write(json.dumps(record, ensure_ascii=False, default=str) + '\n')


def log_metric(paths, run_id, stage, **values):
    """Метрика этапа: строка в logs/metrics.jsonl и gauge в формате Prometheus (logs/metrics.prom)."""
    rec = {'ts': now_utc(), 'run_id': run_id, 'stage': stage, **values}
    _append_jsonl(os.path.join(paths.logs, 'metrics.jsonl'), rec)
    prom = os.path.join(paths.logs, 'metrics.prom')
    lines = {}
    if os.path.exists(prom):
        with open(prom, encoding='utf-8') as fh:
            for line in fh:
                if line.strip() and not line.startswith('#'):
                    lines[line.rsplit(' ', 1)[0]] = line.rstrip('\n')
    for k, v in values.items():
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            key = f'pipeline_{k}{{stage="{stage}"}}'
            lines[key] = f'{key} {v}'
    with open(prom, 'w', encoding='utf-8') as fh:
        fh.write('# TYPE pipeline gauge (последние значения по этапам)\n')
        fh.write('\n'.join(sorted(lines.values())) + '\n')
    return rec


def send_alert(paths, level, kind, message, **context):
    """Алерт: запись в logs/alerts.jsonl и в лог. При заданном SLACK_WEBHOOK_URL уходит и в Slack."""
    rec = {'ts': now_utc(), 'level': level, 'kind': kind, 'message': message, **context}
    _append_jsonl(os.path.join(paths.logs, 'alerts.jsonl'), rec)
    get_logger(paths).warning(f'ALERT [{kind}] {message}')
    url = os.environ.get('SLACK_WEBHOOK_URL')
    if url:
        try:
            import requests
            requests.post(url, json={'text': f'ETL ALERT [{kind}]: {message}'}, timeout=5)
        except Exception as e:  # алерт не должен ронять пайплайн
            get_logger(paths).error(f'Slack недоступен: {e}')
    return rec


def read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding='utf-8') as fh:
        return [json.loads(line) for line in fh if line.strip()]


class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.seconds = time.perf_counter() - self.t0


# Внедрение сбоев для проверки обработки ошибок: {'extract': 2} - первые 2 вызова этапа падают
FAULTS = {}


class TransientError(ConnectionError):
    """Временный сбой (сеть, хранилище): лечится повтором."""


def maybe_fail(stage):
    left = FAULTS.get(stage, 0)
    if left:
        FAULTS[stage] = left - 1
        raise TransientError(f'искусственный сбой этапа {stage} (осталось {left - 1})')


def release_memory():
    """Возвращает ОС освобождённую память (Arrow и glibc держат её у себя после больших пакетов)."""
    import ctypes
    import gc
    gc.collect()
    try:
        import pyarrow as pa
        pa.default_memory_pool().release_unused()
    except Exception:
        pass
    try:
        ctypes.CDLL('libc.so.6').malloc_trim(0)
    except Exception:
        pass
