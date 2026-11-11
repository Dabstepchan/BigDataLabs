
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- У пользователя не должно быть двух рекомендаций с одним рангом или одним товаром
select user_id, count(*) as n, count(distinct rank) as n_ranks, count(distinct product_id) as n_products
from "warehouse"."main"."user_recommendations"
group by user_id
having count(*) <> count(distinct rank) or count(*) <> count(distinct product_id)
  
  
      
    ) dbt_internal_test