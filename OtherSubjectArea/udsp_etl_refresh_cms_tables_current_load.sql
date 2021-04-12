
CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.slicehost.udsp_etl_refresh_cms_tables_current_load()
begin

/*---------------------------------------------------------
--modified 11.11.14_jcmcconnell_moved load check source to ebi-ods-core ebi logging table; added table no column
-----------------------------------------------------------*/
DECLARE BusinessDate datetime;
declare v_loadcheck_no int64;
SET BusinessDate = current_datetime();

---------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.slicehost.ods_tables_current_refresh  WHERE lower(Depency)='cms_report_tables';


create or replace temp table Tables_Current_Load	 as 
SELECT 
    CONCAT(ODS_TABLE_NAME,'-' , `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(BUSINESS_DATE) ) AS Current_Load_NK,
    cast(ODS_TABLE_NAME AS string) AS Table_Name,
    cast(ODS_DB_NAME AS string) AS Source,
    cast(LAST_SUCCESSFULL_LOAD_TS as datetime) AS Current_Record_Date,
    cast(BUSINESS_DATE as datetime) AS MIS_System_Stamp,
    ifnull(LOAD_STATUS,0) AS LOAD_STATUS,	
    Depency,
    In_EBI_Logging																											  
FROM
    `rax-landing-qa`.etl_logging.ods_load_status A
INNER JOIN 
    `rax-abo-72-dev`.slicehost.local_source_table_load B
ON A.ODS_DB_NAME=B.DB
AND A.ODS_TABLE_NAME=Table_Name
INNER JOIN 
   `rax-abo-72-dev`.slicehost.table_load_dependency C
on B.Depency_Key=C.Depency_Key
WHERE
    upper(ODS_DB_NAME) IN ('CMS_ODS','BRM_ODS')
AND LOAD_STATUS = 1
AND B.Depency_Key IN (6,8)
AND BUSINESS_DATE = BusinessDate
order by
    LAST_SUCCESSFULL_LOAD_TS DESC
	;
---------------------------------------------------------------------------------------------------------------------

set v_loadcheck_no= (SELECT ifnull(max(loadcheck_no),0) FROM `rax-abo-72-dev.slicehost.ods_tables_current_refresh`);
INSERT INTO `rax-abo-72-dev`.slicehost.ods_tables_current_refresh
 SELECT
    v_loadcheck_no as loadcheck_no,
    Current_Load_NK,
    Table_Name,
    Source,
    Current_Record_Date,
    MIS_System_Stamp,
    LOAD_STATUS,
    Depency,
    In_EBI_Logging
FROM Tables_Current_Load;

end;
