
    
    

with all_values as (

    select
        model_version as value_field,
        count(*) as n_records

    from "warehouse"."main"."user_recommendations"
    group by model_version

)

select *
from all_values
where value_field not in (
    '1.0'
)


