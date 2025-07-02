select	 
    USER_ID,
	FIRST_NAME,
	LAST_NAME,
	EMAIL,
	SIGNUP_DATE,
	PREFERRED_LANGUAGE,
	DOB,
	MARKETING_OPT_IN,
	ACCOUNT_STATUS,
	loyalty_points_balance
from { { source('de_project', 'user_data') } }