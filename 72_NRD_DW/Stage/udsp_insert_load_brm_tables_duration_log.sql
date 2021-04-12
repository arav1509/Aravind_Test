CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_insert_load_brm_tables_duration_log`()
BEGIN
DECLARE procedure_name STRING DEFAULT 'udsp_bill_invoice_staging_onetime';
DECLARE group_name STRING DEFAULT 'cloud invoice staging';
DECLARE table_name  STRING DEFAULT 'raw_daily_bill_poid_staging,raw_ssis_invoice_load_step1,raw_ssis_event_initial,raw_ssis_event_load_step2,raw_ssis_impacts_load_step3_initial,raw_ssis_impacts_load_step3';
DECLARE start_time datetime DEFAULT CURRENT_DATETIME();
DECLARE duration_time STRING DEFAULT '1';
DECLARE end_time datetime DEFAULT CURRENT_DATETIME();


DELETE FROM stage_one.load_brm_tables_duration_log WHERE 'groupx'=lower(group_name) AND 'procedurex'=lower(procedure_name) AND (Duration_Time = '00:00:00' OR Duration_Time = '1');
---------------------------------------------------------------------------------------
-- IF 
	-- ISNULL((SELECT MAX(Time_Key) FROM Load_BRM_Tables_Duration_Log WHERE [Procedure]=@procedure_name),0)  <> dbo.udf_time_key_nohyphen(getdate())
BEGIN
---------------------------------------------------------------------------------------

  INSERT INTO `rax-staging-dev.stage_one.load_brm_tables_duration_log`
SELECT
  table_name,
	procedure_name,
	`rax-staging-dev.bq_functions.udf_time_key_nohyphen`(CURRENT_DATETIME()) as time_key,
	start_time,
  CAST(DATE(1900,1,1) as DATETIME) as end_time,
  duration_time,
	group_name;

END;

END;
