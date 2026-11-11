
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idempotency_key
from "warehouse"."main"."stg_events"
where idempotency_key is null



  
  
      
    ) dbt_internal_test