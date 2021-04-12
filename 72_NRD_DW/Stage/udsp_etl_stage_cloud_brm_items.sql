CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_stage_cloud_brm_items`()
BEGIN

  DECLARE jobdate DATETIME; 
	DECLARE getdate DATETIME; 
	DECLARE maxdate DATETIME; 
	DECLARE getdate_unix INT64; 

 
---------------------------------------------------------------------------------------   

SET MAXDATE =CAST(current_date()-2 as date) ; 
SET jobdate = cast(maxdate AS DATETIME)   ;
SET GETDATE = jobdate          ;
SET getdate_unix = unix_seconds(CAST(getdate AS TIMESTAMP));
---------------------------------------------------------------------------------------   
--SELECT MAXDATE  
--SELECT jobdate          
--SELECT GETDATE          
--SELECT GETDATE_UNIX          
--*********************************************************************************************************************    
truncate table stage_one.raw_cloud_brm_items ; 
--CALL stage_one.drop_indexes_raw_cloud_brm_items() ;  
--*********************************************************************************************************************      
--SSIS_Event_Load_Step2       
INSERT INTO stage_one.raw_cloud_brm_items(POID_ID0,ITEM_NO,NAME,POID_TYPE,SERVICE_OBJ_TYPE,Bill_Obj_Id0,
AR_BILL_OBJ_ID0,ACCOUNT_OBJ_ID0,ITEM_TOTAL,MOD_DATE,EFFECTIVE_DATE,OPENED_DATE,CREATED_DATE,GL_SEGMENT)
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
    		DATE(TIMESTAMP_SECONDS(cast(mod_t as INT64))) AS MOD_DATE ,
    		DATE(TIMESTAMP_SECONDS(cast(effective_t as INT64))) AS EFFECTIVE_DATE ,   
 DATE(TIMESTAMP_SECONDS(cast(OPENED_T as INT64))) AS OPENED_DATE, 
		DATE(TIMESTAMP_SECONDS(cast(CREATED_T as INT64))) AS CREATED_DATE ,          
		  
    GL_SEGMENT   
FROM (
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
    I.CREATED_T,   
    acct.gl_segment  AS GL_SEGMENT  
FROM   
     `rax-landing-qa`.brm_ods.item_t I
INNER JOIN  
    `rax-landing-qa`.brm_ods.account_t acct
ON I.ACCOUNT_OBJ_ID0= acct.Poid_Id0  
WHERE   
   CAST(I.mod_t AS STRING) >= ' + Convert(Varchar,GETDATE_UNIX) +'
AND 
ITEM_TOTAL<>0    
AND lower(acct.gl_segment) LIKE ('%cloud%'));  

--*********************************************************************************************************************       
DELETE FROM stage_one.raw_cloud_brm_items WHERE lower(name) like '%payment%' ;  
DELETE FROM stage_one.raw_cloud_brm_items WHERE lower(name) like '%writeoff%'  ; 
--*********************************************************************************************************************      
DELETE FROM stage_two_dw.stage_cloud_brm_items WHERE EXISTS (SELECT POID_ID0 FROM  stage_one.raw_cloud_brm_items B WHERE  stage_cloud_brm_items.POID_ID0=B.POID_ID0) ; 
--*********************************************************************************************************************      
INSERT INTO   
    stage_two_dw.stage_cloud_brm_items  
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
    MOD_DATE,          
    EFFECTIVE_DATE,     
    OPENED_DATE,     
    CREATED_DATE,  
    GL_SEGMENT,  
    LEFT(A.ITEM_NO, IFNULL(nullif(STRPOS(',', A.ITEM_NO),0) - 1, 8000))  AS ITEM_BILL_NO  
FROM   
   stage_one.raw_cloud_brm_items A  
WHERE   
    NOT EXISTS (SELECT POID_ID0 FROM  stage_two_dw.stage_cloud_brm_items B WHERE A.POID_ID0=B.POID_ID0)  ;
--*********************************************************************************************************************  

END;
