-- Уже купленный в окне товар не рекомендуется повторно
select r.user_id, r.product_id
from {{ ref('user_recommendations') }} r
join {{ ref('fact_sales') }} s
  on s.user_id = r.user_id and s.product_id = r.product_id
 and s.event_time >= cast('{{ var("as_of") }}' as timestamp) - interval {{ var('window_days') }} day
 and s.event_time < cast('{{ var("as_of") }}' as timestamp)
