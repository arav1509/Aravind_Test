CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_stage_dedicated_brm_items`()
BEGIN

/*Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				rama4760			03/06/2019	 copied from 72 server as part of NRD	  
*/    
---------------------------------------------------------------------------------------            
Declare jobdate datetime;
Declare GETDATE DATETIME;
Declare MAXDATE datetime;
Declare GETDATE_UNIX int64;
Declare TSQL string ;       
---------------------------------------------------------------------------------------     
SET MAXDATE =CAST(DATE_SUB(MAXDATE, INTERVAL -2 DAY) as date)    ;
SET jobdate = DATETIME_TRUNC(DATE_SUB(MAXDATE, INTERVAL -1 DAY), DAY) ;--cast(convert(varchar,MAXDATE-1,101)as datetime)     
SET GETDATE = jobdate ;           
SET GETDATE_UNIX = UNIX_SECONDS(cast(GETDATE as timestamp));-- DATEDIFF(second,{d '1970-01-01'},GETDATE )        
---------------------------------------------------------------------------------------     
--SELECT MAXDATE    
--SELECT jobdate            
--SELECT GETDATE            
--SELECT GETDATE_UNIX            

      
--SSIS_Event_Load_Step2         
create or replace table  stage_one.raw_dedicated_brm_items    as
SELECT DISTINCT    
    POID_ID0,     
    ITEM_NO,     
    NAME,     
    POID_TYPE,     
    SERVICE_OBJ_TYPE,     
    Bill_Obj_Id0,     
    AR_BILL_OBJ_ID0,     
    ACCOUNT_OBJ_ID0,     
    ITEM_TOTAL,
    cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(MOD_T  as int64) second)  as datetime) 	 AS MOD_DATE,  
cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(effective_t  as int64) second)  as datetime)	 AS EFFECTIVE_DATE, 
cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(OPENED_T  as int64) second)  as datetime) AS OPENED_DATE,       
    GL_SEGMENT,
	loaded_date     
FROM     
   (   
SELECT              
    i.POID_ID0,      
    ITEM_NO,    
    i.NAME,          
    I.POID_TYPE,    
    i.SERVICE_OBJ_TYPE,        
    Bill_Obj_Id0,       
    AR_BILL_OBJ_ID0,    
    ACCOUNT_OBJ_ID0,    
    ITEM_TOTAL,    
    I.MOD_T,    
    I.effective_t,    
    I.OPENED_T,     
    I.GL_SEGMENT,
	current_datetime() as loaded_date  
FROM     
     `rax-landing-qa`.brm_ods.item_t I    
INNER JOIN    
    `rax-landing-qa`.brm_ods.account_t acct   
ON I.ACCOUNT_OBJ_ID0= acct.Poid_Id0    
WHERE     
    I.mod_t >= GETDATE_UNIX
AND ITEM_TOTAL<>0      
and account_no like '030%'  
);  

         
DELETE  FROM stage_one.raw_dedicated_brm_items WHERE lower(name) like '%payment%'   ;  
--*********************************************************************************************************************       
DELETE FROM stage_two_dw.stage_dedicated_brm_items WHERE EXISTS (SELECT POID_ID0 FROM  stage_one.raw_dedicated_brm_items B WHERE  stage_dedicated_brm_items.POID_ID0=B.POID_ID0)  ;  
--*********************************************************************************************************************        
INSERT INTO   stage_two_dw.stage_dedicated_brm_items    
SELECT     
    POID_ID0,     
    ITEM_NO,     
    NAME,     
    POID_TYPE,     
    SERVICE_OBJ_TYPE,     
    Bill_Obj_Id0,     
    AR_BILL_OBJ_ID0,     
    ACCOUNT_OBJ_ID0,     
    ITEM_TOTAL,     
    cast(MOD_DATE as date) as MOD_DATE,            
    cast(EFFECTIVE_DATE as date)as EFFECTIVE_DATE,       
    cast(OPENED_DATE  as date) as OPENED_DATE,       
    GL_SEGMENT,    
    LEFT(A.ITEM_NO, ifnull(nullif(strpos(A.ITEM_NO,','),0) - 1, 8000))  AS ITEM_BILL_NO      
FROM     
   stage_one.raw_dedicated_brm_items A    
WHERE     
    NOT EXISTS (SELECT POID_ID0 FROM  stage_two_dw.stage_dedicated_brm_items B WHERE A.POID_ID0=B.POID_ID0)   ; 
--*********************************************************************************************************************       
    
END;
