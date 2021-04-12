CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_complete_net_revenue_oracle_ods_source_tables`()
BEGIN

/*
KVC

It will either confirm that the BRM ODS tables are ready for extraction, or that the job 
needs to try again in 10 minutes before continuing.

Modification History:

*/
/*Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				rama4760			31/05/2019	 copied from 72 server as part of NRD	  
*/ 
DECLARE Table_Count  int64;
DECLARE v_Current_date  int64;
DECLARE Current_Staus  int64;
DECLARE Record_Count  int64;
DECLARE V_Table_Count_check  int64;
DECLARE jobdate datetime;
--------------------------------------------------------------------------------
SET Table_Count=(	SELECT 
				    COUNT(*) 
				FROM 
				    stage_one.netrevenue_local_source_table_load A
				INNER JOIN
				    stage_one.netrevenue_table_load_dependency b
				on A.Depency_Key=b.Depency_Key
				WHERE 
				  	UPPER(DB) IN ('OPERATIONAL_REPORTING_ORACLE','OPERATIONAL_REPORTING_ECONNECT','BRM_ODS')
				AND In_EBI_Logging=1
				AND UPPER(Depency)='ORACLE_DEDICATED_INVOICING');
				
				
SET v_Current_date=(SELECT bq_functions.udf_time_key_nohyphen(MIn(MIS_System_Stamp)) 
				FROM stage_one.ods_tables_current_refresh 
				WHERE In_EBI_Logging=1 
				AND UPPER(Source) IN('OPERATIONAL_REPORTING_ORACLE','OPERATIONAL_REPORTING_ECONNECT','BRM_ODS')
				AND UPPER(Depency)='ORACLE_DEDICATED_INVOICING'
				);

SET Current_Staus=(SELECT (MIn(LOAD_STATUS)) 
					FROM stage_one.ods_tables_current_refresh 
					WHERE In_EBI_Logging=1 
					AND  UPPER(Source) IN('OPERATIONAL_REPORTING_ORACLE','OPERATIONAL_REPORTING_ECONNECT','BRM_ODS') 
					AND  UPPER(Depency)='ORACLE_DEDICATED_INVOICING'
					);
set V_Table_Count_check=(SELECT MAX(loadcheck_no) 
						FROM stage_one.ods_tables_current_refresh 
						WHERE In_EBI_Logging=1 
						AND  UPPER(Source)  IN('OPERATIONAL_REPORTING_ORACLE','OPERATIONAL_REPORTING_ECONNECT','BRM_ODS') 
						AND UPPER(Depency)='ORACLE_DEDICATED_INVOICING'
						);
						
--------------------------------------------------------------------------------   
IF    (Table_Count= V_Table_Count_check) and ( cast(v_Current_date as int64)= cast(bq_functions.udf_time_key_nohyphen(current_datetime()) as int64) and  Current_Staus=1)
then

	select 1;

ELSE

   	--RAISERROR ('Net Revenue ODS Tables Not Updated',16,1)
	--exec dbo.udsp_JOB_Failure_DEV_Team_Email_Alert 'Dedicated Invoice Stage Process Failed due to Tables in the EBI-ODS-CORE.Operational_Reporting_eConnect and EBI-ODS-CORE.dbo.Operational_Reporting_Oracle needed to load Net_Revenue tables have not updated since yesterday'
  select 'udsp_etl_refresh_net_revenue_oracle_ods_tables_current_load sp calling';
	call stage_one.udsp_etl_refresh_net_revenue_oracle_ods_tables_current_load();
  select 'udsp_etl_refresh_net_revenue_oracle_ods_tables_current_load sp executed'; 
  RAISE USING MESSAGE = '<h3 style="background-color:DodgerBlue;">EXCEPTION: Net Revenue ODS Tables Not Updated</h3>'; 


end if;


END;
