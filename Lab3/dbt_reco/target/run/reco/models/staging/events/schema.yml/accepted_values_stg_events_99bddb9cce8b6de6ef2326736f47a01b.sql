
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from "warehouse"."main"."stg_events"
    group by event_type

)

select *
from all_values
where value_field not in (
    'view','cart','purchase','remove_from_cart'
)



  
  
      
    ) dbt_internal_test