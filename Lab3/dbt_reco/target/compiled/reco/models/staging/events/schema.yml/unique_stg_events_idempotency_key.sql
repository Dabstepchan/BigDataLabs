
    
    

select
    idempotency_key as unique_field,
    count(*) as n_records

from "warehouse"."main"."stg_events"
where idempotency_key is not null
group by idempotency_key
having count(*) > 1


