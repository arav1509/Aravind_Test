CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.cloud_uk.udsp_etl_contact_tables_uk()
begin

----1:06
INSERT INTO
	`rax-abo-72-dev`.cloud_uk.act_val_accountstatus
select
	ID, status AS Name, 'Uknonwn' AS Description, 0 AS Online
from 
	`rax-landing-qa`.cms_ods.account_statuses
WHERE
    upper(status_source_system_name)='HMDB_UK'    
AND ID NOT in (select id from `rax-abo-72-dev`.cloud_uk.act_val_accountstatus);
---------------------------------------------------------------------------------------



call `rax-abo-72-dev`.cloud_uk.udsp_etl_cloud_account_contact_info_current();

call `rax-abo-72-dev`.cloud_uk.udsp_etl_cloud_closed_accounts();

call `rax-abo-72-dev`.cloud_uk.udsp_etl_cloud_account_log_status_changes();
---------------------------------------------------------------------------------------


DELETE FROM 
	`rax-abo-72-dev`.cloud_uk.act_log_accountstatus
WHERE
	Date >= `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(date_add(current_date(), INTERVAL -1 MONTH))
AND Date <  `rax-abo-72-dev`.bq_functions.udf_firstdayofnextmonth(current_date()-1)
;

INSERT INTO	 `rax-abo-72-dev`.cloud_uk.act_log_accountstatus
SELECT  
	Date
	   ,cast(ACT_AccountID as int64) as ACT_AccountID
      ,ACT_val_AccountStatusID
      ,PSN_PersonID
      ,Username
FROM (
  SELECT DISTINCT 
	UPDATEDDATE		AS Date,
   	A.ID			AS ACT_AccountID,
	actstatus.ID	AS ACT_val_AccountStatusID,
	NULL			AS PSN_PersonID,
    'N/A'			AS Username	
FROM 
    `rax-landing-qa`.cms_ods.account_history A   
LEFT OUTER JOIN
    `rax-landing-qa`.cms_ods.account_statuses  actstatus   
On A.ACCOUNTSTATUS=actstatus.status
AND upper(status_source_system_name)='HMDB_UK'
WHERE  
	upper(A.type)='CLOUD'
AND ACCOUNTSTATUS is not null
AND	UPDATEDDATE >=  `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(date_add(current_date(),interval -1 month))
AND UPDATEDDATE <date_add(date_trunc(current_date(),month),interval 1 month)
AND upper(ACCOUNTCHANGETYPE) IN (
		'NEW',
		'LEGACY_ACCOUNT_STATUS'
		)
AND  CAST(A.ID as int64) > 10000000
);

end;