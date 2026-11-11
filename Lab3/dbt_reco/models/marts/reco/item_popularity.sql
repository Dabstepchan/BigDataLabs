-- Популярность товаров в категории за окно window_days до момента as_of (модель popular_in_category из ЛР-1):
-- главный критерий - число покупок (плюс корзины с весом cart_weight), при равенстве - корзины и просмотры
with w as (
    select *
    from {{ ref('stg_events') }}
    where event_time >= cast('{{ var("as_of") }}' as timestamp) - interval {{ var('window_days') }} day
      and event_time < cast('{{ var("as_of") }}' as timestamp)
), agg as (
    select
        product_id,
        arg_max(category_id, event_time) as category_id,
        count(*) filter (where event_type = 'purchase') as n_purchases,
        count(*) filter (where event_type = 'cart') as n_carts,
        count(*) filter (where event_type = 'view') as n_views
    from w
    group by product_id
)
select
    *,
    n_purchases + {{ var('cart_weight') }} * n_carts as score,
    row_number() over (partition by category_id order by n_purchases + {{ var('cart_weight') }} * n_carts desc,
                       n_carts desc, n_views desc, product_id) as rank_in_category,
    row_number() over (order by n_purchases + {{ var('cart_weight') }} * n_carts desc,
                       n_carts desc, n_views desc, product_id) as global_rank
from agg
