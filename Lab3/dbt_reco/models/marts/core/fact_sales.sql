-- Факт продаж: одна строка = одна покупка товара
select
    idempotency_key as sale_id,
    event_time,
    event_date,
    user_id,
    product_id,
    category_id,
    category_l1,
    brand,
    price as amount_usd,
    {{ usd_to_rub('price') }} as amount_rub
from {{ ref('stg_events') }}
where event_type = 'purchase'
