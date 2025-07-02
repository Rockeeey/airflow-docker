SELECT cart_id,
       search_event_id,
       session_id,
       journey_id,
       cart_id,
       search_terms,
       search_results,
       search_type
       timestamp,
       dbt_load_timestamp
from {{ ref('stg_user_journey_rk') }}