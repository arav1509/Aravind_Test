CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_refresh_dedicated_brm_tables_current_load`()
BEGIN
/*---------------------------------------------------------  
--modified 11.11.14_jcmcconnell_moved load check source to ebi-ods-core ebi logging table; added table no column  
-----------------------------------------------------------*/----------------------------------------------------------------------------------------------------------------------  
/*Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				rama4760			30/05/2019	 copied from 72 server as part of NRD	  
*/ 

----------------------------------------------------------------------------------------------------------------------  
DECLARE v_loadcheck_no int64;  
SET v_loadcheck_no = (select ifnull(max(loadcheck_no),0)+1 from  stage_one.ods_tables_current_refresh );
----------------------------------------------------------------------------------------------------------------------  
DELETE FROM `rax-staging-dev`.stage_one.ods_tables_current_refresh  WHERE UPPER(Depency)='BRM_DEDICATED_INVOICING' ;
---------------------------------------------------------------------------------------------------------------------  
INSERT INTO `rax-staging-dev`.stage_one.ods_tables_current_refresh (loadcheck_no,  
    Current_Load_NK,  
    Table_Name,  
    Source,  
    Current_Record_Date,  
    MIS_System_Stamp,  
    LOAD_STATUS,  
    Depency,  
    In_EBI_Logging  ) 
 SELECT  
    loadcheck_no,  
    Current_Load_NK,  
    Table_Name,  
    Source,  
    Current_Record_Date,  
    MIS_System_Stamp,  
    LOAD_STATUS,  
    Depency,  
    In_EBI_Logging  
FROM  (
    SELECT   --#Tables_Current_Load   
	v_loadcheck_no as loadcheck_no,
    CONCAT(ODS_TABLE_NAME,'-' , `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(BUSINESS_DATE) )  AS Current_Load_NK,  
    cast(ODS_TABLE_NAME AS STRING)                     AS Table_Name,  
    cast(ODS_DB_NAME AS STRING)                      AS Source,  
    cast(LAST_SUCCESSFULL_LOAD_TS as datetime)                    AS Current_Record_Date,  
    cast(BUSINESS_DATE as datetime)                      AS MIS_System_Stamp,  
    IfNULL(LOAD_STATUS,1)                        AS LOAD_STATUS,   
    Depency,  
    In_EBI_Logging                               
FROM  
  `rax-landing-qa`.etl_logging.ods_load_status A  
INNER JOIN   
   `rax-staging-dev`.stage_one.slicehost_local_source_table_load B  
ON  lower(A.ODS_DB_NAME)=lower(B.DB )
AND lower(A.ODS_TABLE_NAME)=lower(Table_Name)  
INNER JOIN   
   `rax-staging-dev`.stage_one.slicehost_table_load_dependency C  
on B.Depency_Key=C.Depency_Key  
WHERE  
    (upper(ODS_DB_NAME) IN ('BRM_ODS') and  task_name like '%bq_final') or   (upper(ODS_DB_NAME) in ('OPERATIONAL_REPORTING_ORACLE')   )
AND LOAD_STATUS = 0  
AND B.Depency_Key=1  
AND CAST(business_date AS DATE) = CURRENT_DATE()
order by  
    LAST_SUCCESSFULL_LOAD_TS DESC  );
END;
