
        
            delete from "warehouse"."main"."stg_events"
            where (
                idempotency_key) in (
                select (idempotency_key)
                from "stg_events__dbt_tmp20261010091618377769"
            );

        
    

    insert into "warehouse"."main"."stg_events" ("idempotency_key", "event_time", "event_date", "event_type", "product_id", "category_id", "category_code", "category_l1", "brand", "price", "user_id", "user_session")
    (
        select "idempotency_key", "event_time", "event_date", "event_type", "product_id", "category_id", "category_code", "category_l1", "brand", "price", "user_id", "user_session"
        from "stg_events__dbt_tmp20261010091618377769"
    )
  