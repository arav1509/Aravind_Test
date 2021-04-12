CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.cloud_uk.udsp_etl_cloud_closed_accounts()
begin

create or replace table  `rax-abo-72-dev`.cloud_uk.cloud_closed_accounts as 
SELECT DISTINCT
	CAST(Account_ID as string)									AS Account_Number,
	CAST(concat(CAST(Account_ID as string),'','Cloud_Hosting_UK') as string)				
																		AS Account_Key,
	Account_Status														AS Account_Status,
	Account_End_Date													AS Account_End_Date,
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Account_End_Date)						AS Account_End_Date_Time_Month
FROM
	`rax-abo-72-dev`.cloud_uk.cloud_account_contact_info_current 
WHERE
	lower(Account_Status)='closed'	
;

end;
