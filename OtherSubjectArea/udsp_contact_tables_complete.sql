CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_contact_tables_complete()
begin

--- set date parameters:
Declare v_date datetime;
Declare v_Getdate datetime;
---------------------------------------------------------

-- check if slicehost table is refreshed today
Set v_Getdate = (
			 SELECT
				CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(MIN(MAX_Date)) as datetime)
			 FROM
			 (
				SELECT 
					IfNULL(MAX(Refresh_Date),'1900-01-01')		AS MAX_Date
				FROM
					`rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A  
			-----------
			 UNION ALL
			-- --------
				SELECT 
					IfNULL(MAX(Refresh_Date),'1900-01-01')		AS MAX_Date
				FROM
				    `rax-abo-72-dev`.cloud_uk.cloud_account_contact_info_current A  
			-----------
			 UNION ALL
			-- --------
				SELECT 
					IfNULl(MAX(Refresh_Date),'1900-01-01')		AS MAX_Date
				FROM
				    `rax-abo-72-dev`.mailtrust.email_apps_core_primary_contact_info A  	    
				    
			)A
			);
Set v_date =`rax-abo-72-dev`.bq_functions.udfdatepart(current_datetime());  -- today, no time

--PRINT month(@Date)
--PRINT day(@date) 
--PRINT year(@date)
--Print v_date
--Print v_Getdate


-- if table is ready, go; else send error message
if v_Getdate = v_date then
	select 'Success';
else
	--exec udsp_JOB_Failure_Email_Alert_HMDB 'slicehost db Cloud contact tables not ready for Sales load JOB '
	RAISE USING MESSAGE = '<h3 style="background-color:DodgerBlue;">EXCEPTION: slicehost db Cloud contact tables not ready for Sales load JOB ';
	
end if;
end;