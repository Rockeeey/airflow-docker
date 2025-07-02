select mkt_campaign,
       mkt_medium,
       mkt_source,
       mkt_content,
       product_id,
       search_event_id,
       dbt_loaded_at

from { { source('de_project', 'user_journey') } }