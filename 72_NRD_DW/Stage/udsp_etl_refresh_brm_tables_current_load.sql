CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_refresh_brm_tables_current_load`()
BEGIN  
	/*---------------------------------------------------------  
	--modified 11.11.14_jcmcconnell_moved load check source to ebi-ods-core ebi logging table; added table no column  
	-----------------------------------------------------------*/----------------------------------------------------------------------------------------------------------------------  
	DECLARE businessdate DATE;  
	DECLARE max_key INT64;
	SET businessdate = CURRENT_DATE();  
	SET max_key = (SELECT MAX(COALESCE(loadcheck_no,0)) FROM `rax-staging-dev`.stage_one.ods_tables_current_refresh);
	DELETE FROM `rax-staging-dev`.stage_one.ods_tables_current_refresh  WHERE lower(depency) = 'brm_cloud_invoicing'  ;
	----------------------------------------------------------------------------------------------------------------------  
	---------------------------------------------------------------------------------------------------------------------  
	INSERT INTO `rax-staging-dev`.stage_one.ods_tables_current_refresh(loadcheck_no,
current_load_nk,table_name, source,current_record_date,mis_system_stamp,load_status,depency,in_ebi_logging)
SELECT  
loadcheck_no,      
current_load_nk,
    Table_Name, 
    Source,  
    current_record_date,  
    mis_system_stamp,  
    load_status, 
    depency,  
    in_ebi_logging  
FROM  ( SELECT  
		max_key+1 AS loadcheck_no,
		--CAST(CONCAT(ods_table_name , '-' , CAST(`rax-staging-dev.bq_functions.udf_yearmonth_nohyphen`(CAST(businessdate AS DATETIME)) AS STRING)) AS STRING) AS current_load_nk,  
    cast(concat(ods_table_name,'-',`rax-staging-dev.bq_functions.udf_yearmonth_nohyphen`(business_date)) as string)  as current_load_nk,
		CAST(ods_table_name AS STRING)                     AS table_name,  
		CAST(ods_db_name AS STRING)                      AS source,  
		CAST(last_successfull_load_ts AS DATETIME)                    AS current_record_date,  
		CAST(business_date AS DATETIME)                      AS mis_system_stamp,  
		COALESCE(load_status,1)                        AS load_status, 
    
		depency,  
		in_ebi_logging                               
	FROM `rax-landing-qa.etl_logging.ods_load_status` A  
	INNER JOIN  `rax-staging-dev`.stage_one.slicehost_local_source_table_load B  
		ON  lower(A.ods_db_name) =    lower(B.db ) 
		AND lower(A.ods_table_name) = lower(B.table_name  )
	INNER JOIN `rax-staging-dev`.stage_one.slicehost_table_load_dependency C  
	on B.depency_key = C.depency_key  
	WHERE  UPPER(A.ods_db_name) = 'BRM_ODS' and  task_name like '%bq_final'
		AND A.load_status = 0  
		AND B.depency_key=2  
		and CAST(business_date AS DATE) = CURRENT_DATE()
		);     
 
END;
