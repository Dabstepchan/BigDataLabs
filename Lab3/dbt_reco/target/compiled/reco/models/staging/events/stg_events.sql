-- Staging: очистка и нормализация событий. Инкрементальная модель с unique_key:
-- повторный запуск по тем же дням перезаписывает строки (delete+insert), а не дублирует их.


select
    idempotency_key,
    event_time,
    event_date,
    event_type,
    product_id,
    category_id,
    coalesce(nullif(trim(category_code), ''), 'unknown') as category_code,
    split_part(category_code, '.', 1) as category_l1,
    coalesce(nullif(lower(trim(brand)), ''), 'unknown') as brand,
    price,
    user_id,
    user_session
from delta_scan('/home/php-docker/www/labs/BigData/BigData_3/work/sandbox_checks/lake/raw_events')
where price > 0

  and event_date >= cast(cast('2019-10-01 10:00:00' as timestamp) as date)
  and event_date <= cast(cast('2019-10-01 12:00:00' as timestamp) as date)
