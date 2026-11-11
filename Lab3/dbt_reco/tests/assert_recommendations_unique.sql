-- У пользователя не должно быть двух рекомендаций с одним рангом или одним товаром
select user_id, count(*) as n, count(distinct rank) as n_ranks, count(distinct product_id) as n_products
from {{ ref('user_recommendations') }}
group by user_id
having count(*) <> count(distinct rank) or count(*) <> count(distinct product_id)
