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
FROM { { source('de_project', 'user_data') } }