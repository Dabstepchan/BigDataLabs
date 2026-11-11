
    

    create  table
      "warehouse"."main"."fact_sales__dbt_tmp"
  
    
    as (
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
    round(price * 84.43, 2) as amount_rub
from "warehouse"."main"."stg_events"
where event_type = 'purchase'
    );
    
  