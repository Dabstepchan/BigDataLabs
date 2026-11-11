"""Transform (ELT): запуск dbt-проекта поверх DuckDB и чтение витрин."""
import json
import os
import subprocess
import sys

import duckdb
import psutil

from .common import maybe_fail

DBT_DIR = os.environ.get('LAB3_DBT_DIR', 'dbt_reco')

# Production-модель, с которой должны быть согласованы витрины (реестр в state/production_model.json)
PRODUCTION_MODEL = {'model_name': 'popular_in_category', 'model_version': '1.0', 'k': 10, 'window_days': 10,
                    'cart_weight': 0, 'exclude_bought': True}


def production_model(paths):
    f = os.path.join(paths.state, 'production_model.json')
    if not os.path.exists(f):
        with open(f, 'w') as fh:
            json.dump(PRODUCTION_MODEL, fh, indent=1)
    with open(f) as fh:
        return json.load(fh)


def dbt_env(paths, warehouse=None):
    env = dict(os.environ)
    total_mb = psutil.virtual_memory().total / 2**20
    mem_mb = int(total_mb * (0.12 if total_mb >= 8 * 1024 else 0.08))   # при малой RAM DuckDB чаще пишет на диск
    env.update({
        'LAB3_WAREHOUSE': os.path.abspath(warehouse or paths.warehouse),
        'LAB3_RAW_PATH': os.path.abspath(paths.raw),
        'LAB3_DUCKDB_MEM': f'{mem_mb}MB',
        'LAB3_DBT_THREADS': '2' if psutil.virtual_memory().total >= 8 * 2**30 else '1',
        'DBT_PROFILES_DIR': os.path.abspath(DBT_DIR),
        'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false',
    })
    return env


def run_dbt(paths, args, dbt_vars=None, warehouse=None, check=True):
    """dbt в отдельном процессе: файл DuckDB открыт только у одного писателя."""
    maybe_fail('transform')
    cmd = [sys.executable, '-m', 'dbt.cli.main', *args, '--project-dir', os.path.abspath(DBT_DIR)]
    if dbt_vars:
        cmd += ['--vars', json.dumps(dbt_vars)]
    p = subprocess.run(cmd, env=dbt_env(paths, warehouse), capture_output=True, text=True)
    out = p.stdout + p.stderr
    results = []
    rr = os.path.join(DBT_DIR, 'target', 'run_results.json')
    if os.path.exists(rr) and args[0] in ('build', 'run', 'test'):
        with open(rr) as fh:
            for r in json.load(fh)['results']:
                results.append({'node': r['unique_id'], 'status': r['status'],
                                'seconds': round(r['execution_time'], 2),
                                'failures': r.get('failures'), 'message': r.get('message')})
    if check and p.returncode != 0:
        raise RuntimeError(f'dbt {args[0]} завершился с кодом {p.returncode}:\n' + out[-3000:])
    return {'returncode': p.returncode, 'results': results, 'log': out}


def model_vars(paths, as_of, batch_start, model=None):
    model = model or production_model(paths)
    return {'as_of': as_of, 'batch_start': batch_start, 'k': model['k'], 'window_days': model['window_days'],
            'model_name': model['model_name'], 'model_version': model['model_version'],
            'cart_weight': model.get('cart_weight', 0), 'exclude_bought': model.get('exclude_bought', True)}


def query(paths, sql, params=None, warehouse=None):
    con = duckdb.connect(warehouse or paths.warehouse, read_only=True)
    try:
        return con.execute(sql, params or []).fetch_arrow_table()
    finally:
        con.close()
