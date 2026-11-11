# Лабораторная работа №3
# Выполнил студент группы МОАм-251 Козырев А.Д.

Промышленный ELT-пайплайн для рекомендательной системы: часовые пакеты кликстрима проходят извлечение, проверку качества с DLQ, загрузку в Delta Lake через MERGE, трансформацию в dbt, проверки Great Expectations и публикацию рекомендаций. Пошаговая реализация и результаты - в ноутбуке `Lab_BigData_3.ipynb`.

**Вариант 5 (последняя цифра зачётки):** E-commerce Clickstream, файл `2019-Oct.csv` из набора [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (Kaggle).

**Бизнес-сценарий:** 2. Рекомендательная система - ежечасное обновление рекомендаций, задержка обработки < 45 минут, согласованность с production-моделью, fallback на предыдущие рекомендации.

**Оформление:** вариант B, Jupyter Notebook.

## Запуск

Ноутбук рассчитан на запуск с нуля («Выполнить всё») в Google Colab или в любом окружении с Python 3.10+:

```bash
pip install -r requirements.txt
jupyter nbconvert --to notebook --execute Lab_BigData_3.ipynb   # или открыть и выполнить все ячейки
```

Первая ячейка ставит недостающие пакеты, следующая скачивает `2019-Oct.csv` через `kagglehub`, если файла нет рядом с ноутбуком. Промежуточные данные (landing, Delta Lake, DuckDB, журналы) создаются в `work/`; при повторном запуске уже обработанные партиции берутся оттуда.

Тесты и шаги CI запускаются и отдельно:

```bash
pytest -q tests                                   # идемпотентность и DLQ на тестовых данных
python -m pipeline.ci gx --root work              # чекпоинт Great Expectations
python -m pipeline.ci dbt-test --root work        # dbt test
python smoke_test.py --warehouse work/staging.duckdb
```

## Структура

```
BigData_3/
├── Lab_BigData_3.ipynb          ноутбук с пошаговой реализацией и выводами
├── README.md, requirements.txt
├── pipeline/                    этапы: extract, quality (DLQ, schema drift, GX), storage (Delta MERGE, time travel),
│                                transform (dbt), deploy и ci (CI/CD), common (логи, метрики, алерты)
├── prefect_flows/etl_pipeline.py  оркестрация в Prefect (flow reco_etl, подпоток ingest_partition)
├── dbt_reco/                    проект dbt: models/staging, models/marts/{core,reco}, macros, tests
├── gx/                          конфигурация Great Expectations (создаётся при запуске)
├── tests/test_pipeline.py       проверки идемпотентности и DLQ, tests/data - тестовые данные
├── smoke_test.py                smoke-тест версии перед production
├── experiments/etl_vs_elt.py    сравнение затрат ETL и ELT
└── .github/workflows/ci.yml     GitHub Actions: test -> deploy-staging -> deploy-production (ревью)
```

## Краткие выводы

_[TODO_COLAB: вывод по результатам прогона]_
