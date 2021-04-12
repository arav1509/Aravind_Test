CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_stage_cloud_email_apps_gl_codes`()
BEGIN
  
/*Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				anil2912			07/06/2019	 copied from 72 server as part of NRD	  
*/  
--------------------------------------------------------------------------------  
------------------------------------------------------------------------------------------------------------------------  
DECLARE GL_BeginDate datetime;
DECLARE GL_EndDate DATETIME; 
DECLARE BEGINTDATE_UNIX  INT64; 
DECLARE ENDTDATE_UNIX  INT64;  
DECLARE TSQL STRING   ;

SET GL_BeginDate=bq_functions.udf_firstdayofyear(DATE_ADD( CURRENT_DATE(),INTERVAL -5 YEAR)) ;  -- In SQL Server its qual to current_datetime, but in BQ its current_date(), if we put current_datetime() getting error
Set GL_EndDate = bq_functions.udf_lastdayofmonth(CURRENT_DATETIME())  ;
SET BEGINTDATE_UNIX = DATETIME_DIFF(DATETIME(GL_BeginDate),cast("1970-01-01" as DATE), SECOND); --DATEDIFF(second,{d '1970-01-01'},GL_BeginDate )  ;
SET ENDTDATE_UNIX =  DATETIME_DIFF(DATETIME(GL_EndDate),cast("1970-01-01" as DATE), SECOND); --DATEDIFF(second,{d '1970-01-01'},GL_EndDate )  ;

--SELECT GL_BeginDate,GL_EndDate,BEGINTDATE_UNIX,ENDTDATE_UNIX

-------------------------------------------------------------------------------------------------------------------------  

CREATE OR REPLACE TABLE stage_one.raw_cloud_apps_brm_account_info AS 
SELECT DISTINCT  
    ACCOUNT_NO,  
    CURRENCY    AS Currency_Code,  
     CANON_COMPANY,  
    GL_SEGMENT,  
    COMPANY     
FROM (
SELECT DISTINCT  
    ACCOUNT_NO,  
    C.CURRENCY,  
    CANON_COMPANY,  
    GL_SEGMENT,  
    COMPANY  
FROM   
 `rax-landing-qa`.brm_ods.account_t  A   
INNER JOIN  
 `rax-landing-qa`.brm_ods.account_nameinfo_t B    
ON POID_ID0=OBJ_ID0  
AND LOWER(GL_SEGMENT) Like '%.emailapps%'  
LEFT OUTER JOIN  
 `rax-landing-qa`.brm_ods.ifw_currency  C    
ON A.CURRENCY=C.CURRENCY_ID  
WHERE  
 COMPANY IS NOT NULL) ; 

create or replace table stage_one.raw_email_apps_gl_account_stage  as
SELECT * -- Stage_One.Raw_Email_Apps_GL_Account_Stage  
FROM (
SELECT DISTINCT  
 A.ACCOUNT_NO,  
 GL_ID,   
 ITEM_NAME  
FROM  
 `rax-landing-qa`.brm_ods.ledger_report_accts_t  A   
LEFT OUTER JOIN  
 `rax-landing-qa`.brm_ods.config_glid_t  B   
ON A.GL_ID=B.REC_ID  
WHERE  
 A.ACCOUNT_NO not like '%-222512%'  
AND (lower(ITEM_NAME) like '%sa3 feed%' or lower(ITEM_NAME) like '%adjustment%' OR lower(ITEM_NAME) like '%write%')  
AND A.EFFECTIVE_T >= BEGINTDATE_UNIX AND A.EFFECTIVE_T  <=ENDTDATE_UNIX);

SELECT DISTINCT  --  INTO  #GL_Products 
 GL_ID       AS GL_Code,  
 IfNULL(glid_descr,'Unknown') AS GL_Description,  
 null       AS GL_Term,  
 CASE   
  WHEN ITEM_NAME like '%SA3 Feed%' then 'INV'  
  WHEN ITEM_NAME like '%Adjustment%' then 'ADJ'  
  WHEN ITEM_NAME like '%Write%' then 'Write-Off'  
 END        AS GL_Type,  
 -1        AS Include_in_payable,  
 -1        AS Include_In_QC  
FROM  
    stage_one.raw_cloud_apps_brm_account_info  AI  
INNER JOIN  
 stage_one.raw_email_apps_gl_account_stage A   
ON AI.ACCOUNT_NO=A.ACCOUNT_NO  
INNER JOIN  
  stage_one.raw_brm_glid_account_config GL  
ON A.GL_ID=GL.glid_rec_id  
AND AI.GL_SEGMENT=GLSeg_name  ;
-------------------------------------------------------------------------------------------------------------  
INSERT INTO  stage_two_dw.stage_cloud_email_apps_gl_codes  
 (  
 GL_Code, GL_Description, GL_Term, GL_Type, GL_Product_Group, Include_in_payable, Include_In_QC  
 )  
SELECT  DISTINCT  
 GL_Code,  
 GL_Description,  
 GL_Term,  
 GL_Type    AS GL_Type,  
 'Unknown'   AS GL_Product_Group,  
 Include_In_Payable,  
 Include_In_QC  
FROM   
 (
		SELECT DISTINCT  --  INTO  #GL_Products 
			 cast(GL_ID  as string)      AS GL_Code,  
			 IfNULL(glid_descr,'Unknown') AS GL_Description,  
			 null       AS GL_Term,  
			 CASE   
			  WHEN ITEM_NAME like '%SA3 Feed%' then 'INV'  
			  WHEN ITEM_NAME like '%Adjustment%' then 'ADJ'  
			  WHEN ITEM_NAME like '%Write%' then 'Write-Off'  
			 END        AS GL_Type,  
			 -1        AS Include_in_payable,  
			 -1        AS Include_In_QC  
		FROM  
				stage_one.raw_cloud_apps_brm_account_info  AI  
		INNER JOIN  
			 stage_one.raw_email_apps_gl_account_stage A   
			ON AI.ACCOUNT_NO=A.ACCOUNT_NO  
		INNER JOIN  
			  stage_one.raw_brm_glid_account_config GL  
			ON A.GL_ID=GL.glid_rec_id  
			AND AI.GL_SEGMENT=GLSeg_name
 )--#GL_Products  
WHERE   
 GL_Code NOT IN  
  (  
  SELECT DISTINCT   
   cast(GL_Code   as string )
  FROM  
   stage_two_dw.stage_cloud_email_apps_gl_codes   
  )  ;
-------------------------------------------------------------------------------------------------------------  

UPDATE  stage_two_dw.stage_cloud_email_apps_gl_codes  
SET   
  GL_Description=B.GL_Description   
FROM    
 stage_two_dw.stage_cloud_email_apps_gl_codes   A   
INNER JOIN  
 (
		SELECT DISTINCT  --  INTO  #GL_Products 
			 GL_ID       AS GL_Code,  
			 IfNULL(glid_descr,'Unknown') AS GL_Description,  
			 null       AS GL_Term,  
			 CASE   
			  WHEN ITEM_NAME like '%SA3 Feed%' then 'INV'  
			  WHEN ITEM_NAME like '%Adjustment%' then 'ADJ'  
			  WHEN ITEM_NAME like '%Write%' then 'Write-Off'  
			 END        AS GL_Type,  
			 -1        AS Include_in_payable,  
			 -1        AS Include_In_QC  
		FROM  
				stage_one.raw_cloud_apps_brm_account_info  AI  
		INNER JOIN  
			 stage_one.raw_email_apps_gl_account_stage A   
			ON AI.ACCOUNT_NO=A.ACCOUNT_NO  
		INNER JOIN  
			  stage_one.raw_brm_glid_account_config GL  
			ON A.GL_ID=GL.glid_rec_id  
			AND AI.GL_SEGMENT=GLSeg_name
 )B --#GL_Products B   
ON A.GL_Code= cast(B.GL_Code   as string)
WHERE  
     A.GL_Description<>B.GL_Description   ;
END;
