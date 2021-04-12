CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.slicehost.udsp_cms_complete()
begin
/*
It will either confirm that the BRM ODS tables are ready for extraction, or that the job 
needs to try again in 10 minutes before continuing.

lmarshal 20160608 - added below query in proc comments for easy troublshooting of what table(s) are blocking

select top 10 a.* from [ebi-ods-core].ebi_logging.dbo.ods_load_status a with(nolock)
inner join (
select * from Local_Source_Table_Load A
INNER JOIN 
   dbo.Table_Load_Dependency B
on A.Depency_Key=B.Depency_Key
WHERE B.Depency_Key=6 --BRM_Cloud_Invoicing
and table_name not in (select table_name from ODS_Tables_Current_Refresh WHERE [DB]='BRM_ODS')
) b on a.ods_table_name = b.table_name and a.ods_db_name = b.[db]
order by last_successfull_load_ts desc

Modification History:
20180715 RV exclude table 'Customer_Account_Metadata'
02122019 CHANDRA PUTTA	-- CORRECTED THE ASSIGNMENT STATEMENT FOR v_Current_date from MIN to MAX
*/
--------------------------------------------------------------------------------
DECLARE v_Table_Count  int64;
DECLARE v_Current_date  int64;
DECLARE v_Current_Staus  int64;
DECLARE v_Record_Count  int64;
DECLARE v_jobdate datetime;
--------------------------------------------------------------------------------
SET v_Table_Count=(	SELECT 
				    COUNT(*) 
				FROM 
				    `rax-abo-72-dev`.slicehost.local_source_table_load A
				INNER JOIN
				     `rax-abo-72-dev`.slicehost.table_load_dependency b
				on A.Depency_Key=b.Depency_Key
				WHERE 
				  	In_EBI_Logging=1
				AND lower(Depency) IN ('cms_report_tables','brm_us_uk_contact_info_current') and lower(table_name) not like 'customer_account_metadata')
				;
				
SET v_Current_date=(SELECT `rax-abo-72-dev`.bq_functions.udf_time_key_nohyphen(MAX(MIS_System_Stamp)) FROM `rax-abo-72-dev`.slicehost.ods_tables_current_refresh WHERE In_EBI_Logging=1 AND lower(Depency) IN ('cms_report_tables','brm_us_uk_contact_info_current'));

SET v_Current_Staus=(SELECT (MIn(LOAD_STATUS)) FROM `rax-abo-72-dev`.slicehost.ods_tables_current_refresh WHERE In_EBI_Logging=1 AND lower(Depency) IN ('cms_report_tables','brm_us_uk_contact_info_current'));
--------------------------------------------------------------------------------   
IF
    v_Table_Count=(SELECT MAX(loadcheck_no) FROM `rax-abo-72-dev`.slicehost.ods_tables_current_refresh WHERE In_EBI_Logging=1 AND lower(Depency) IN ('cms_report_tables','brm_us_uk_contact_info_current')) AND v_Current_date=(SELECT `rax-abo-72-dev`.bq_functions.udf_time_key_nohyphen(current_date())) AND v_Current_Staus=1 then
	select 1;

ELSE
	RAISE USING MESSAGE = '<h3 style="background-color:DodgerBlue;">EXCEPTION: Load_Cloud_Contact_Tables. Step: CMS Availability. CMS tables in the [EBI-ODS-CORE].[CMS_ODS] and BRM Table in [EBI-ODS-CORE].[BRM_ODS] needed to load both Cloud_UK and Slicehost Cloud_Account_Contact_Info_Current have not updated since yesterday';

end if;
end;