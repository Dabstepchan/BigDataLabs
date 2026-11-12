"""Определения фич Feast: снимки статистик пользователя и товара за 7 дней на начало каждого дня."""
import os
from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource, ValueType
from feast.types import Float64

DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

user = Entity(name='user', join_keys=['user_id'], value_type=ValueType.INT64, description='Пользователь магазина')
product = Entity(name='product', join_keys=['product_id'], value_type=ValueType.INT64, description='Товар')

user_source = FileSource(name='user_stats_source', path=os.path.join(DATA, 'user_stats.parquet'),
                         timestamp_field='event_timestamp', created_timestamp_column='created')
product_source = FileSource(name='product_stats_source', path=os.path.join(DATA, 'product_stats.parquet'),
                            timestamp_field='event_timestamp', created_timestamp_column='created')

# ttl = 1 день: снимок действует до следующего снимка; более старые значения считаются отсутствующими
user_stats_7d = FeatureView(
    name='user_stats_7d', entities=[user], ttl=timedelta(days=1), online=True, source=user_source,
    schema=[Field(name=n, dtype=Float64) for n in (
        'u_views_7d', 'u_carts_7d', 'u_purchases_7d', 'u_sessions_7d', 'u_active_days_7d',
        'u_avg_price_7d', 'u_cart_rate_7d', 'u_purchase_rate_7d')],
    tags={'team': 'reco', 'window': '7d'},
)
product_stats_7d = FeatureView(
    name='product_stats_7d', entities=[product], ttl=timedelta(days=1), online=True, source=product_source,
    schema=[Field(name=n, dtype=Float64) for n in (
        'p_views_7d', 'p_carts_7d', 'p_purchases_7d', 'p_users_7d', 'p_avg_price_7d',
        'p_cart_rate_7d', 'p_purchase_rate_7d')],
    tags={'team': 'reco', 'window': '7d'},
)
reco_ranker_v1 = FeatureService(name='reco_ranker_v1', features=[user_stats_7d, product_stats_7d])
