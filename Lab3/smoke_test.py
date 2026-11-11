"""Smoke-тест версии перед production: рекомендации есть, списки полные, поиск по user_id < 100 мс."""
import argparse
import json
import sys

from pipeline.deploy import smoke_test

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--k', type=int, default=10)
    args = ap.parse_args()
    res = smoke_test(args.warehouse, k=args.k)
    print(json.dumps(res, ensure_ascii=False, indent=1))
    sys.exit(0 if res['passed'] else 1)
