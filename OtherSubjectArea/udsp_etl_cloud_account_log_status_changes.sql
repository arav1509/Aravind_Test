CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.slicehost.udsp_etl_cloud_account_log_status_changes()
begin
create or replace temp table data_Temp as
SELECT 
    modifiedDate			  AS Date, 
	CAST(NULL as datetime)	  AS Status_Change_Date,
	Null					  AS Duration_Seconds,
	id					  AS ACT_AccountID, 
	ACCOUNT_STATUS, 
	'N/A' 				  AS PSN_PersonID, 
	'N/A'  				  AS Username

FROM (
SELECT DISTINCT 
   	A.ID			AS ID,
	ACCOUNTSTATUS	AS ACCOUNT_STATUS,
	UPDATEDDATE	AS MODIFIEDDATE,
	A.TYPE		AS TYPE
FROM 
    `rax-landing-qa`.cms_ods.account_history A   
WHERE  
	upper(A.type)='CLOUD'
AND upper(ACCOUNTCHANGETYPE) IN (
		'NEW',
		'LEGACY_ACCOUNT_STATUS'
		)
AND  CAST(A.ID as int64) < 10000000
)
;

update data_Temp A
set A.Status_Change_Date =(
SELECT 
		TS1.Date
	FROM 
		data_Temp TS1 , data_Temp A
	WHERE 
		TS1.ACT_AccountID=A.ACT_AccountID 
	AND TS1.Date > A.Date 
	ORDER BY
		TS1.Date
    limit 1 )
    where true;
-------------------------------------------------------------------------------------------------------------------
create or replace table `rax-abo-72-dev`.slicehost.cloud_account_log_status_changes as
SELECT DISTINCT 
	ACT_AccountID, 
	Date												AS Status_Change_Start_Date, 
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Date) AS Status_Change_Start_Time_Month_Key, 
	Status_Change_Date AS Status_Change_Update_Date,
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Status_Change_Date) AS Status_Change_Update_Time_Month_Key, 
	ifnull(datetime_diff(Date,Status_Change_Date,SECOND),0) AS Duration_Seconds,
	ID											     AS Account_Status_ID, 
	B.Name											AS Account_Status,
	PSN_PersonID 
FROM 
	data_Temp A
INNER JOIN
	`rax-abo-72-dev`.slicehost.act_val_accountstatus B
ON A.ACCOUNT_STATUS=Name
;

end;