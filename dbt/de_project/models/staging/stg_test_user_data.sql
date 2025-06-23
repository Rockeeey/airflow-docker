-- models/staging/stg_test_user_data.sql
SELECT
    user_id,
    user_name,
    email,
    date_joined
FROM {{ source('de_project', 'user_data') }}
