"""Команды CI/CD: их вызывают шаги .github/workflows/ci.yml (python -m pipeline.ci <команда>)."""
import argparse
import json
import os
import sys

from . import deploy, quality as q, transform as tr
from .common import get_paths


def last_batch(paths):
    with open(os.path.join(paths.state, 'last_transform.json')) as fh:
        return json.load(fh)


def candidate(paths):
    with open(os.path.join(paths.state, 'candidate_model.json')) as fh:
        return json.load(fh)


def run_gx(paths, warehouse=None):
    model = tr.production_model(paths)
    ctx = q.gx_context()
    q.ensure_gx_objects(ctx, k=model['k'], model_version=model['model_version'])
    reco = tr.query(paths, """SELECT r.user_id, r.rank, r.product_id, r.model_version,
                                     p.product_id IS NOT NULL AS product_in_dim
                              FROM user_recommendations r LEFT JOIN dim_products p USING (product_id)""",
                    warehouse=warehouse).to_pandas()
    return q.run_checkpoint(ctx, 'recommendations', reco)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('cmd', choices=['sample-run', 'gx', 'dbt-test', 'deploy-staging', 'smoke', 'canary', 'promote',
                                    'rollback'])
    ap.add_argument('--root', default=os.environ.get('LAB3_ROOT', 'work'))
    ap.add_argument('--share', type=int, default=5)
    args = ap.parse_args(argv)
    os.environ['LAB3_ROOT'] = os.path.abspath(args.root)
    paths = get_paths(args.root)
    staging = paths.p('staging.duckdb')
    if args.cmd == 'sample-run':
        import shutil
        from prefect_flows.etl_pipeline import reco_etl
        from .extract import list_partitions
        shutil.copytree(os.path.join('tests', 'data', 'landing'), paths.landing, dirs_exist_ok=True)
        res = reco_etl(partitions=list_partitions(paths))
        ok = res['status'] == 'ok'
    elif args.cmd == 'gx':
        res = run_gx(paths)
        ok = res['success']
    elif args.cmd == 'dbt-test':
        b = last_batch(paths)
        res = tr.run_dbt(paths, ['test'], tr.model_vars(paths, b['as_of'], b['batch_start']), check=False)
        res = {'returncode': res['returncode'], 'failed': [r for r in res['results'] if r['status'] != 'pass']}
        ok = res['returncode'] == 0
    elif args.cmd == 'deploy-staging':
        b = last_batch(paths)
        res = deploy.build_version(paths, candidate(paths), staging, b['as_of'], b['batch_start'])
        ok = res['ok']
    elif args.cmd == 'smoke':
        res = deploy.smoke_test(staging)
        ok = res['passed']
    elif args.cmd == 'canary':
        res = deploy.canary(paths, paths.warehouse, staging, last_batch(paths)['as_of'], share=args.share)
        ok = res['promote']
    elif args.cmd == 'promote':
        deploy.promote(paths, candidate(paths), 'canary passed')
        res, ok = {'promoted': candidate(paths)}, True
    else:
        res = deploy.rollback(paths, 'CI: провал canary или деплоя')
        ok = True
    print(json.dumps(res, ensure_ascii=False, default=str, indent=1))
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
