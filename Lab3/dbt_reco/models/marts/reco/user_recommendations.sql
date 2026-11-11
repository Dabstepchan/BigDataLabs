-- Рекомендации для пользователей, активных в пакете [batch_start, as_of):
-- топ-k товаров категории интереса (категория с наибольшим взвешенным числом действий за сутки),
-- без уже купленных в окне товаров; если товаров категории не хватает - добор из глобального топа.
-- Списки собираются операциями над массивами: так не нужен оконный проход по кандидатам всех пользователей.
with active as (
    select distinct user_id
    from {{ ref('stg_events') }}
    where event_time >= cast('{{ var("batch_start") }}' as timestamp)
      and event_time < cast('{{ var("as_of") }}' as timestamp)
), recent as (
    select s.user_id, s.product_id, s.category_id, s.event_type, s.event_time
    from {{ ref('stg_events') }} s
    semi join active a on a.user_id = s.user_id
    where s.event_time >= cast('{{ var("as_of") }}' as timestamp) - interval {{ var('window_days') }} day
      and s.event_time < cast('{{ var("as_of") }}' as timestamp)
), interest as (
    select user_id, arg_max(category_id, w * 1e12 + epoch(last_t)) as category_id
    from (
        select user_id, category_id, sum({{ event_weight('event_type') }}) as w, max(event_time) as last_t
        from recent
        where event_time >= cast('{{ var("as_of") }}' as timestamp) - interval 1 day
          and category_id is not null
        group by user_id, category_id
    )
    group by user_id
), bought as (
    select user_id, list(distinct product_id) as items
    from recent
    where event_type = 'purchase'
    group by user_id
), cat_top as (
    select category_id, list(product_id order by rank_in_category) as items
    from {{ ref('item_popularity') }}
    where rank_in_category <= {{ var('k') }} * 2
    group by category_id
), global_top as (
    select list(product_id order by global_rank) as items
    from {{ ref('item_popularity') }}
    where global_rank <= {{ var('k') }} * 2
), lists as (
    select
        a.user_id,
        list_filter(coalesce(c.items, []), lambda x: not ({{ var('exclude_bought') }} and list_contains(coalesce(b.items, []), x))) as cat_ok,
        list_filter(g.items, lambda x: not ({{ var('exclude_bought') }} and list_contains(coalesce(b.items, []), x))
                                   and not list_contains(coalesce(c.items, []), x)) as glob_ok
    from active a
    left join interest i on i.user_id = a.user_id
    left join cat_top c on c.category_id = i.category_id
    left join bought b on b.user_id = a.user_id
    cross join global_top g
), final as (
    select user_id, len(cat_ok) as n_cat, list_slice(cat_ok || glob_ok, 1, {{ var('k') }}) as items
    from lists
), flat as (
    select user_id, n_cat, unnest(items) as product_id, generate_subscripts(items, 1) as pos
    from final
)
select
    f.user_id,
    cast(f.pos as integer) as rank,
    f.product_id,
    p.category_id,
    cast(p.score as double) as score,
    case when f.pos <= f.n_cat then 'category' else 'global' end as source,
    '{{ var("model_name") }}' as model_name,
    '{{ var("model_version") }}' as model_version,
    cast('{{ var("as_of") }}' as timestamp) as generated_at
from flat f
join {{ ref('item_popularity') }} p on p.product_id = f.product_id
