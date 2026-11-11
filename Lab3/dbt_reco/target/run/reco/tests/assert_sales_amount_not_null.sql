
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Сумма продажи в рублях заполнена для каждой покупки
select * from "warehouse"."main"."fact_sales" where amount_rub is null
  
  
      
    ) dbt_internal_test