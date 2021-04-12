CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_update_load_brm_tables_duration_log`()
BEGIN  

DECLARE procedure_name STRING DEFAULT 'udsp_bill_invoice_staging_onetime';
DECLARE start_time datetime DEFAULT CURRENT_DATETIME();
DECLARE duration_time STRING DEFAULT '1';
DECLARE end_time datetime DEFAULT CURRENT_DATETIME();

update   
 stage_one.load_brm_tables_duration_log  
SET   
 End_Time= CURRENT_DATETIME()
WHERE   
 Duration_time='1' AND 'procedurex'=lower(procedure_name) AND `rax-staging-dev.bq_functions.udf_time_key_nohyphen`(start_time)=`rax-staging-dev.bq_functions.udf_time_key_nohyphen`(CURRENT_DATETIME()) ;
---------------------------------------------------------------------------------------  
Update   
 stage_one.load_brm_tables_duration_log  
SET  
 duration_time=  
(  
CASE  
 WHEN  
 -- `rax-staging-dev`.bq_functions.udftimeinterval(CAST(IFNULL((date_diff(start_time,end_time,SEC)),0)as int64)) <= '00:00:01'  
  		 `rax-staging-dev.bq_functions.udftimeinterval`(cast(COALESCE((DATETIME_DIFF(end_time,start_time,SECOND)),0)as int64)) < '00:00:01'
 THEN  
  '00:00:02'  
 ELSE   
  		 `rax-staging-dev.bq_functions.udftimeinterval`(cast(COALESCE((DATETIME_DIFF(end_time,start_time,SECOND )),0)as int64))
END  
)  
WHERE   
 Duration_time='1'  AND 'procedurex'=lower(procedure_name) AND `rax-staging-dev.bq_functions.udf_time_key_nohyphen`(Start_Time)=`rax-staging-dev.bq_functions.udf_time_key_nohyphen`(CURRENT_DATETIME()); 
END;
