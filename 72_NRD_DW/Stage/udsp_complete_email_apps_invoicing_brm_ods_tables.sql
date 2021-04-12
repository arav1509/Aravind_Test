CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_complete_email_apps_invoicing_brm_ods_tables`()
BEGIN

/* It will either confirm that the BRM ODS tables are ready for extraction, or that the job needs to try again in 10 minutes before continuing.  
Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				rama4760			30/05/2019	 copied from 72 server	  
*/  

DECLARE Table_Count  int64;  
DECLARE v_Current_date  int64;  
DECLARE Current_Staus  int64;  
DECLARE Record_Count  int64;  
DECLARE jobdate datetime ; 
DECLARE v_date int64;
--------------------------------------------------------------------------------  
SET Table_Count=(  
    SELECT   
        COUNT(*)  
    FROM   
        stage_one.netrevenue_local_source_table_load  A  
    inner join  
        stage_one.netrevenue_table_load_dependency b  
    on A.Depency_Key=b.Depency_Key  
    WHERE   
        lower(DB)='brm_ods'   
    AND lower(Depency)='brm_email_apps_invoicing') ;

	
SET v_Current_date=(SELECT cast(bq_functions.udf_time_key_nohyphen(MIn(MIS_System_Stamp)) as int64) FROM stage_one.ods_tables_current_refresh WHERE upper(source)='BRM_ODS' AND  upper(Depency)='BRM_EMAIL_APPS_INVOICING');

  
SET Current_Staus=(SELECT (MIn(LOAD_STATUS)) FROM stage_one.ods_tables_current_refresh WHERE upper(source)='BRM_ODS' AND upper(Depency)='brm_email_apps_invoicing')  ;


SET jobdate = (select(datetime_sub(current_datetime(), interval 4 day)));  
--------------------------------------------------------------------------------  

set v_date = cast(bq_functions.udf_time_key_nohyphen(current_date()) as int64);
IF  
    (Table_Count=(SELECT MAX(loadcheck_no) FROM stage_one.ods_tables_current_refresh WHERE upper(source)='BRM_ODS' AND          upper(Depency)='brm_email_apps_invoicing' )
    ) AND Current_Staus=1 and   (v_Current_date =v_date )
    
    
then  
 select 1  ;
  
ELSE  

 
  --RAISERROR ('BRM ODS Tables Not Updated',16,1)  
 -- exec dbo.udsp_JOB_Failure_DEV_Team_Email_Alert 'Email & Apps Invoice Stage Process Failed due to BRM tables in the EBI-ODS-CORE.BRM_ODS needed to load Email_Apps_Invoicing tables have not updated since yesterday'  
  call stage_one.udsp_etl_refresh_email_apps_invoicing_brm_ods_tables_current_load(); 
  RAISE USING MESSAGE = '<h3 style="background-color:DodgerBlue;">Email & Apps Invoice Stage Process Failed due to BRM tables in the [EBI-ODS-CORE].[BRM_ODS] needed to load Email_Apps_Invoicing tables have not updated since yesterday</h3>';
  end if;
--------------------------------------------------------------------------------  
END;
