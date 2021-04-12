CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_brm_inv_detail_tables`()
BEGIN  
	DECLARE bgn_Key INT64;  
	DECLARE end_Key INT64;  
	----------------------------------------------------------------------------------------  
	SET bgn_Key = `rax-staging-dev.bq_functions.udf_time_key_nohyphen`(DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -90 DAY))  ;
	----------------------------------------------------------------------------------------  
	DELETE FROM stage_one.load_brm_tables_duration_log WHERE `groupx` = 'cloud invoice staging' AND (duration_time = '00:00:00' OR duration_time = '1')  ;
	----------------------------------------------------------------------------------------  
	DELETE FROM  stage_one.load_brm_tables_duration_log  
	WHERE time_key < bgn_key ;  
	--------------------------------------------------------------------------------  
	
CALL `rax-staging-dev.stage_two_dw.udsp_etl_stage_cloud_brm_items`();

--------------------------------------------------------------------------------
--udsp_etl_Raw_Cloud_BRM_Items_Aggregate
---------------------------------------------------------------------------------------


CALL `rax-staging-dev.stage_one.udsp_etl_raw_cloud_brm_items_aggregate`();

--------------------------------------------------------------------------------
--udsp_etl_Load_Raw_InvItemEventDetail_Daily_Stage
---------------------------------------------------------------------------------------


CALL `rax-staging-dev.stage_one.udsp_etl_load_raw_invitemeventdetail_daily_stage`();


--------------------------------------------------------------------------------
--udsp_etl_Load_Stage_InvItemEventDetail_Incremental
---------------------------------------------------------------------------------------

CALL `rax-staging-dev.stage_two_dw.udsp_etl_load_stage_invitemeventdetail_incremental`();


END;
