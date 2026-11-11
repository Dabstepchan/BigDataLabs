
    
    

select
    sale_id as unique_field,
    count(*) as n_records

from "warehouse"."main"."fact_sales"
where sale_id is not null
group by sale_id
having count(*) > 1


