CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_refresh_net_revenue_oracle_ods_tables_current_load`()
BEGIN

/*
	CALL stage_one.udsp_etl_refresh_net_revenue_oracle_ods_tables_current_load ();
*/

----------------------------------------------------------------------------------------------------------------------
DECLARE businessdate DATETIME;
DECLARE max_key INT64 DEFAULT 0;

SET businessdate = CAST(CURRENT_DATE() AS DATETIME);

----------------------------------------------------------------------------------------------------------------------
	DELETE FROM `rax-staging-dev.stage_one.ods_tables_current_refresh`  
		WHERE LOWER(Source) IN ('ebs_ods','ebs_ods', 'brm_ods') 
			AND LOWER(Depency) = 'oracle_dedicated_invoicing';
		

----------------------------------------------------------------------------------------------------------------------
	SET max_key = (SELECT COALESCE(MAX(loadcheck_no),0) + 1 FROM stage_one.ods_tables_current_refresh) ;
	
INSERT INTO `rax-staging-dev.stage_one.ods_tables_current_refresh`(loadcheck_no,Current_Load_NK,Table_Name,Source,current_record_date, mis_system_stamp, load_status, depency, in_ebi_logging	)
select
max_key as loadcheck_no,
Current_Load_NK,
Table_Name,
Source,
current_record_date,
 mis_system_stamp,
 load_status,
 depency,
 in_ebi_logging	
from(
SELECT
		CONCAT(ods_table_name, '-' , `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(business_date)) AS Current_Load_NK,
		CAST(ods_table_name AS STRING) AS Table_Name,
		CAST(ods_db_name AS STRING) AS Source,
		CAST(last_successfull_load_ts AS DATETIME) AS current_record_date,
		CAST(business_date AS DATETIME) AS mis_system_stamp,
		COALESCE(load_status,1) AS load_status,	
		depency,
		in_ebi_logging																											  
	FROM
		`rax-landing-qa.etl_logging.ods_load_status` A
	INNER JOIN `rax-staging-dev.stage_one.netrevenue_local_source_table_load` B
		ON LOWER(A.ods_db_name) = LOWER(CASE WHEN lower(B.DB)='operational_reporting_oracle' THEN 'ebs_ods' ELSE lower(B.DB) END)
		AND LOWER(A.ods_table_name) = LOWER(Table_Name)
	INNER JOIN `rax-staging-dev.stage_one.netrevenue_table_load_dependency` C
		ON B.depency_Key = C.depency_Key
	WHERE
		LOWER(ods_db_name) = 'ebs_ods'
		AND load_status = 0
		AND B.Depency_Key = 2
		AND CAST(business_date AS DATE) = CURRENT_DATE()
union all
	SELECT 
		CONCAT(ods_table_name, '-' , `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(business_date)) AS Current_Load_NK,
		CAST(ods_table_name AS STRING) AS Table_Name,
		CAST(ods_db_name AS STRING) AS Source,
		CAST(Load_end_time_UTC AS DATETIME) AS current_record_date,
		CAST(business_date AS DATETIME) AS mis_system_stamp,
		COALESCE(load_status,1) AS load_status,	
		Depency,
		In_EBI_Logging
		
	FROM `rax-landing-qa.etl_logging.ods_load_status` A
	INNER JOIN `rax-staging-dev.stage_one.netrevenue_local_source_table_load` B
		ON LOWER(A.ods_db_name) = LOWER(B.DB)
		AND LOWER(A.ods_table_name) = LOWER(Table_Name)
	INNER JOIN `rax-staging-dev.stage_one.netrevenue_table_load_dependency` C
	ON B.Depency_Key = C.Depency_Key
	WHERE
		LOWER(ods_db_name) = 'operational_reporting_econnect'
	AND load_status = 0
	AND B.Depency_Key = 2

union all
	SELECT 
		CONCAT(ods_table_name, '-' , `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(business_date)) AS Current_Load_NK,
		CAST(ods_table_name AS STRING) AS Table_Name,
		CAST(ods_db_name AS STRING) AS Source,
		CAST(Load_end_time_UTC AS DATETIME) AS current_record_date,
		CAST(business_date AS DATETIME) AS mis_system_stamp,
		COALESCE(load_status,1) AS load_status,	
		Depency,
		In_EBI_Logging			
	FROM `rax-landing-qa.etl_logging.ods_load_status` A
	INNER JOIN `rax-staging-dev.stage_one.netrevenue_local_source_table_load` B
		ON LOWER(A.ods_db_name) = LOWER(B.DB)
		AND LOWER(A.ods_table_name) = LOWER(Table_Name)
	INNER JOIN `rax-staging-dev.stage_one.netrevenue_table_load_dependency` C
		ON B.Depency_Key = C.Depency_Key
	WHERE
		LOWER(ods_db_name) = 'brm_ods' and task_name like '%bq_final'
		AND load_status = 0
		AND B.Depency_Key = 2
		AND CAST(business_date AS DATE) = CURRENT_DATE()
	);
END;
