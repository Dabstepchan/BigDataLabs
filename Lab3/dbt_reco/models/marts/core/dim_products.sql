-- Измерение товаров: последние известные категория, бренд и цена
select
    product_id,
    arg_max(category_id, event_time) as category_id,
    arg_max(category_code, event_time) as category_code,
    arg_max(category_l1, event_time) as category_l1,
    arg_max(brand, event_time) as brand,
    arg_max(price, event_time) as last_price_usd,
    count(*) filter (where event_type = 'purchase') as n_purchases
from {{ ref('stg_events') }}
group by product_id
