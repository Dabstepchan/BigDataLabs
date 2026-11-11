-- Измерение пользователей: активность и средний чек
select
    user_id,
    min(event_time) as first_seen,
    max(event_time) as last_seen,
    count(*) as n_events,
    count(*) filter (where event_type = 'view') as n_views,
    count(*) filter (where event_type = 'cart') as n_carts,
    count(*) filter (where event_type = 'purchase') as n_purchases,
    coalesce(sum(price) filter (where event_type = 'purchase'), 0) as revenue_usd,
    sum(price) filter (where event_type = 'purchase') / nullif(count(distinct user_session) filter (where event_type = 'purchase'), 0)
        as avg_check_usd
from {{ ref('stg_events') }}
group by user_id
