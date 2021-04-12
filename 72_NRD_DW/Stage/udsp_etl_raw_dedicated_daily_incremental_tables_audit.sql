CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_dedicated_daily_incremental_tables_audit`()
BEGIN


declare bgn_key int64;
declare end_key int64;
--[Ashok: Added below declaration to insert into load_brm_tables_duration_log from select clause in line 50 ]
DECLARE table_name STRING DEFAULT 'dedicate_brm_invoice_incremental_stage_tables';
DECLARE procedure_name STRING DEFAULT 'udsp_etl_raw_dedicated_brm_rev_daily_incremental_audit';
DECLARE start_time datetime DEFAULT CURRENT_DATETIME();
DECLARE duration_time STRING DEFAULT '1';
DECLARE `group` STRING DEFAULT 'brm_dedicated_audit';
DECLARE group_name DEFAULT 'BRM Dedicated Audit';

SET bgn_key = bq_functions.udf_time_key_nohyphen(DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 92 DAY));


----------------------------------------------------------------------------------------

delete from stage_one.load_brm_tables_duration_log WHERE lower(`group`)=lower(group_name) and (duration_time = '00:00:00' or duration_time = '1');
----------------------------------------------------------------------------------------

SELECT FORMAT('The value of bgn_key of %d ', bgn_key) AS result;

delete from
	stage_one.load_brm_tables_duration_log
WHERE
	time_key <	bgn_key;

---------------------------------------------------------------------------------------
 
----------------------------------------------------------------------------------------
--udsp_etl_Dedicated_BRM_Rev_Daily_Incremental
---------------------------------------------------------------------------------------

if 
	COALESCE((SELECT max(time_key) from stage_one.load_brm_tables_duration_log WHERE procedure=procedure_name),0)  <> bq_functions.udf_time_key_nohyphen(CURRENT_DATETIME())
  THEN
  

INSERT INTO 
	stage_one.load_brm_tables_duration_log
SELECT
	table_name,
	procedure_name,
	bq_functions.udf_time_key_nohyphen(CURRENT_DATETIME()),
	start_time,
	CAST(DATE(1900,1,1) as DATETIME),
	duration_time,
	group_name;

END if;
CALL stage_one.udsp_etl_raw_dedicated_brm_rev_daily_incremental_audit();

UPDATE 
	stage_one.load_brm_tables_duration_log 
SET 
	end_time= CURRENT_DATETIME()
WHERE 
	procedure=procedure_name and bq_functions.udf_time_key_nohyphen(start_time)=bq_functions.udf_time_key_nohyphen(CURRENT_DATETIME());	
---------------------------------------------------------------------------------------

UPDATE 
	stage_one.load_brm_tables_duration_log 
SET
	duration_time=
(
case
	when
		bq_functions.udftimeinterval(cast(COALESCE((DATETIME_DIFF(end_time,start_time ,SECOND)),0)as int64)) < '00:00:01'
	then
		'00:00:02'
	else	
		bq_functions.udftimeinterval(cast(COALESCE((DATETIME_DIFF(end_time,start_time,SECOND )),0)as int64))
end
)
WHERE 
	lower(procedure)=procedure_name and bq_functions.udf_time_key_nohyphen(start_time)=bq_functions.udf_time_key_nohyphen(CURRENT_DATETIME());

END;
