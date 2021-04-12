CREATE or replace procedure `rax-staging-dev`.stage_one.udsp_etl_dedicated_brm_inv_detail_tables()
begin
---------------------------------------------------------------------------------------
/*
Created On: 9/18/2017
Created By: Kano Cannick

Description:
Runs all the procs needed to populate report tables need to stage Dedicated BRM Invoice tables.

*/
/*Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				rama4760			03/06/2019	 copied from 72 server as part of NRD and removed the unwanted Objects.	
												 created SPs for insert and update for Load_BRM_Tables_Duration_Log Table 
*/


----------------------------------------------------------------------------------------
--udsp_etl_Raw_BRM_DEDICATED_ACCOUNT_PROFILE_load
---------------------------------------------------------------------------------------



call stage_one.udsp_etl_raw_brm_dedicated_account_profile_load();



----------------------------------------------------------------------------------------
--udsp_etl_raw_brm_cloud_account_profile_load
---------------------------------------------------------------------------------------



call stage_one.udsp_etl_raw_brm_cloud_account_profile_load();



----------------------------------------------------------------------------------------
--udsp_etl_raw_brm_glid_account_config
---------------------------------------------------------------------------------------



call stage_one.udsp_etl_raw_brm_glid_account_config();



----------------------------------------------------------------------------------------
--udsp_etl_stage_dedicated_brm_items
---------------------------------------------------------------------------------------



call `rax-staging-dev`.stage_two_dw.udsp_etl_stage_dedicated_brm_items();


----------------------------------------------------------------------------------------
--udsp_etl_raw_dedicated_brm_items_aggregate
---------------------------------------------------------------------------------------



call stage_one.udsp_etl_raw_dedicated_brm_items_aggregate();



----------------------------------------------------------------------------------------
--udsp_etl_raw_brm_dedicated_invoice_aggregate_total
---------------------------------------------------------------------------------------



call stage_one.udsp_etl_raw_brm_dedicated_invoice_aggregate_total();



----------------------------------------------------------------------------------------
--udsp_etl_stage_payment_term
---------------------------------------------------------------------------------------



call stage_two_dw.udsp_etl_stage_payment_term();



----------------------------------------------------------------------------------------
--udsp_etl_raw_brm_dedicated_billing_daily_stage_master
---------------------------------------------------------------------------------------



call stage_one.udsp_etl_raw_brm_dedicated_billing_daily_stage_master();




end;
