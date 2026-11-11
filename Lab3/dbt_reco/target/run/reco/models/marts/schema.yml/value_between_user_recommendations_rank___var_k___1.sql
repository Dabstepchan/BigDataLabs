
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
select * from "warehouse"."main"."user_recommendations" where rank < 1 or rank > 10

  
  
      
    ) dbt_internal_test