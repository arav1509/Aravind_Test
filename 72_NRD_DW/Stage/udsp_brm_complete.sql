CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_brm_complete`()
BEGIN

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
WHERE B.Depency_Key=2 --BRM_Cloud_Invoicing  
and table_name not in (select table_name from ODS_Tables_Current_Refresh WHERE [DB]='BRM_ODS')  
) b on a.ods_table_name = b.table_name and a.ods_db_name = b.[db]  
order by last_successfull_load_ts desc  
  
Modification History:  
*/  

/*Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				rama4760			30/05/2019	 copied from 72 server	  
*/ 


--------------------------------------------------------------------------------  
DECLARE Table_Count  int64 ;  
DECLARE v_Current_date  int64 ;  
DECLARE Current_Staus  int64 ;  
DECLARE Record_Count  int64 ;  
DECLARE jobdate datetime ; 
--------------------------------------------------------------------------------  
SET Table_Count=( SELECT   
        COUNT(*)   
    FROM   
        `rax-staging-dev`.stage_one.slicehost_local_source_table_load A  
    INNER JOIN  
        `rax-staging-dev`.stage_one.slicehost_table_load_dependency b  
    on A.Depency_Key=b.Depency_Key  
    WHERE   
       In_EBI_Logging=1  
    AND lower(Depency)='brm_cloud_invoicing'
	) ;
	
SET v_Current_date=(SELECT `rax-staging-dev`.bq_functions.udf_time_key_nohyphen(MIN(MIS_System_Stamp)) FROM `rax-staging-dev`.stage_one.ods_tables_current_refresh WHERE In_EBI_Logging=1 AND lower(Depency)='BRM_Cloud_Invoicing');
  
SET Current_Staus=(SELECT (MIn(LOAD_STATUS)) FROM `rax-staging-dev`.stage_one.ods_tables_current_refresh WHERE In_EBI_Logging=1 AND lower(Depency)='brm_cloud_invoicing')  ;
--------------------------------------------------------------------------------     
IF  
    Table_Count=(SELECT MAX(loadcheck_no) FROM `rax-staging-dev`.stage_one.ods_tables_current_refresh WHERE In_EBI_Logging=1 AND lower(Depency)='brm_cloud_invoicing') AND v_Current_date=(SELECT `rax-staging-dev`.bq_functions.udf_time_key_nohyphen(current_date())) AND Current_Staus=1   
then  
 select 1  ;
  
ELSE    
	call `rax-staging-dev.stage_one`.udsp_etl_refresh_brm_tables_current_load();
   RAISE USING MESSAGE =  ('Job: Cloud Invoice Stage Process failed due to BRM Source tables not updated in [EBI-ODS-CORE].[BRM_ODS] since yesterday')  ;
end if;    
 
END;
