
    
    



select idempotency_key
from "warehouse"."main"."stg_events"
where idempotency_key is null


