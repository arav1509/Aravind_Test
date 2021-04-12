CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_refresh_email_apps_invoicing_brm_ods_tables_current_load`()
BEGIN
  
/*modified 11.11.14_jcmcconnell_moved load check source to ebi-ods-core ebi logging table; added table no column  

Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				rama4760			30/05/2019	 copied from 72 server	*/  
    

delete from `rax-staging-dev`.stage_one.ods_tables_current_refresh  where lower(source)='brm_ods' and lower(depency)='brm_email_apps_invoicing';
----------------------------------------------------------------------------------------------------------------------  

INSERT INTO  
   `rax-staging-dev`.stage_one.ods_tables_current_refresh(loadcheck_no,Current_Load_NK,  Table_Name, Source,Current_Record_Date,  MIS_System_Stamp,  LOAD_STATUS,  Depency,  In_EBI_Logging    )
SELECT    loadcheck_no,Current_Load_NK,
    Table_Name, 
    Source,  
    Current_Record_Date,  
    MIS_System_Stamp,  
    LOAD_STATUS,  
    Depency,  
    In_EBI_Logging  
FROM  (
    select --#Tables_Current_Load 
    row_number() over ( ) as loadcheck_no,
	cast(concat(ods_table_name,'-',`rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(BUSINESS_DATE)) as string)  as current_load_nk,
    cast(ods_table_name as string) as table_name,
    cast(ods_db_name as string) as source,
    cast(last_successfull_load_ts as datetime) as current_record_date,
    cast(business_date as datetime) as mis_system_stamp ,
    coalesce(load_status,1) as load_status,
    depency, 
    in_ebi_logging																													  
from
     `rax-landing-qa`.etl_logging.ods_load_status a
inner join
    `rax-staging-dev`.stage_one.netrevenue_local_source_table_load b
on lower(a.ods_db_name)=lower(b.db)
and lower(a.ods_table_name)=lower(table_name)
inner join 
   `rax-staging-dev`.stage_one.netrevenue_table_load_dependency  c
on b.depency_key=c.depency_key
where
    lower(ods_db_name) = 'brm_ods'  and task_name like '%bq_final'
and load_status = 0
and b.depency_key=1
AND CAST(business_date AS DATE) = CURRENT_DATE()
order by
    last_successfull_load_ts desc
	); 

END;
