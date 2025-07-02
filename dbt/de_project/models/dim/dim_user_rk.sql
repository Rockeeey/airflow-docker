-- create dimensional table USER
SELECT user_id,
    first_name,
    last_name,
    email,
    signup_date,
    preferred_language,
    dob,
    marketing_opt_in,
    account_status,
    loyalty_points_balance
from {{ ref('stg_user_data_rk') }}