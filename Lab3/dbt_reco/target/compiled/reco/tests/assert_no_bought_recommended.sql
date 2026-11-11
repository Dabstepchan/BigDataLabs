-- Уже купленный в окне товар не рекомендуется повторно
select r.user_id, r.product_id
from "warehouse"."main"."user_recommendations" r
join "warehouse"."main"."fact_sales" s
  on s.user_id = r.user_id and s.product_id = r.product_id
 and s.event_time >= cast('2019-10-01 12:00:00' as timestamp) - interval 10 day
 and s.event_time < cast('2019-10-01 12:00:00' as timestamp)