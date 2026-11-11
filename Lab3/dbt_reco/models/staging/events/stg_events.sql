-- Staging: очистка и нормализация событий. Инкрементальная модель с unique_key:
-- повторный запуск по тем же дням перезаписывает строки (delete+insert), а не дублирует их.
{{ config(materialized='incremental', unique_key='idempotency_key', incremental_strategy='delete+insert') }}

select
    idempotency_key,
    event_time,
    event_date,
    event_type,
    product_id,
    category_id,
    coalesce(nullif(trim(category_code), ''), 'unknown') as category_code,
    {{ category_level('category_code', 1) }} as category_l1,
    coalesce(nullif(lower(trim(brand)), ''), 'unknown') as brand,
    price,
    user_id,
    user_session
from {{ source('lake', 'raw_events') }}
where price > 0
{% if is_incremental() %}
  and event_date >= cast(cast('{{ var("batch_start") }}' as timestamp) as date)
  and event_date <= cast(cast('{{ var("as_of") }}' as timestamp) as date)
{% endif %}
