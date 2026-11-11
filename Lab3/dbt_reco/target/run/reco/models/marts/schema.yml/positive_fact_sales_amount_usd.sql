
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
select * from "warehouse"."main"."fact_sales" where amount_usd <= 0

  
  
      
    ) dbt_internal_test