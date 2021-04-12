CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_stage_dedicated_gl_codes_brm`()
BEGIN


CREATE OR REPLACE TEMP TABLE cloud_gl_account_description AS
SELECT DISTINCT   
    GL.glsub3_acct_subprod AS GL_Code,  
    cast('Unknown'as  STRING) AS GL_Account_Description,  
    '' AS GL_Account_Group,  
    '' AS GL_Calculation_Rule_Category,  
    '' AS GL_Global_Net_Revenue_Bucket_Desc,  
    0 AS GL_Is_Net_Revenue,  
    0 AS GL_Is_Dedicated_Revenue  
FROM  
    `rax-staging-dev.stage_one.raw_invitemeventdetail_daily_stage` A
left outer join  
    `rax-staging-dev.stage_one.raw_brm_glid_account_config` GL  
    ON A.EBI_GL_ID= glid_rec_id  
AND A.GL_SEGMENT= GL.GLSeg_name  
inner join   
    `rax-staging-dev.stage_two_dw.stage_brm_glid_acct_atttribute` atr  
on GL.GLAcct_attribute= atr.Attribute_id  
inner join   
    `rax-staging-dev.stage_two_dw.stage_brm_glid_acct_report_type` typ  
ON GL.GLAcct_record_type=typ.Report_Type_id  
 
AND GL.GLAcct_record_type IN (2,8) -- gl for BILLED events only  
AND GL.GLAcct_attribute= 1   

UNION ALL

SELECT DISTINCT   
    GL.glsub3_acct_subprod AS GL_Code,  
    cast('Unknown'as  STRING) AS GL_Account_Description,  
    '' AS GL_Account_Group,  
    '' AS GL_Calculation_Rule_Category,  
    '' AS GL_Global_Net_Revenue_Bucket_Desc,  
    0 AS GL_Is_Net_Revenue,  
    0 AS GL_Is_Dedicated_Revenue  
  
FROM  
    `rax-staging-dev.stage_one.raw_dedicated_inv_event_detail` A
left outer join  
     `rax-staging-dev.stage_one.raw_brm_glid_account_config` GL
     ON A.glid_rec_id= GL.glid_rec_id  
AND A.GL_SEGMENT= GL.GLSeg_name
inner join   
    `rax-staging-dev.stage_two_dw.stage_brm_glid_acct_atttribute` atr  
on GL.GLAcct_attribute= atr.Attribute_id  
inner join   
     `rax-staging-dev.stage_two_dw.stage_brm_glid_acct_report_type` typ  
ON GL.GLAcct_record_type=typ.Report_Type_id  
   
AND GL.GLAcct_record_type IN (2,8) -- gl for BILLED events only  
AND GL.GLAcct_attribute= 1 ;  

-----------------------------------------------------------------------------------------------------------       
-- INSERT INTO  
    -- #Cloud_GL_Account_Description  
-- SELECT DISTINCT   
    -- GL.glsub3_acct_subprod AS GL_Code,  
    -- cast('Unknown'as  STRING) AS GL_Account_Description,  
    -- '' AS GL_Account_Group,  
    -- '' AS GL_Calculation_Rule_Category,  
    -- '' AS GL_Global_Net_Revenue_Bucket_Desc,  
    -- 0 AS GL_Is_Net_Revenue,  
  -- 0 AS GL_Is_Dedicated_Revenue  
  
-- FROM  
    -- Dedicated_Inv_Event_Detail_Stage_Audit A with(nolock)  
-- left outer join  
    -- slicehost.Dim_BRM_GLID_ACCOUNT_CONFIG GL   
-- inner join   
    -- slicehost.Dim_BRM_GLID_ACCT_Atttribute atr  
-- on GL.GLAcct_attribute= atr.Attribute_id  
-- inner join   
    -- slicehost.Dim_BRM_GLID_ACCT_Report_Type typ  
-- ON GL.GLAcct_record_type=typ.Report_Type_id  
-- ON A.glid_rec_id= GL.glid_rec_id  
-- AND A.GL_SEGMENT= GL.GLSeg_name   
-- AND GL.GLAcct_record_type IN (2,8) -- gl for BILLED events only  
-- AND GL.GLAcct_attribute= 1   
-- WHERE  
      -- GL.glsub3_acct_subprod NOT IN (SELECT GL_Code FROM #Cloud_GL_Account_Description)  
-----------------------------------------------------------------------------------------------------------  
/** BRM GL SEGMENT DETAILS **/  
/** Oracle GL Values ***/  

CREATE OR REPLACE TEMP TABLE flex_values AS  
SELECT   
    flex_value,  
    ffvt.DESCRIPTION,  
    flex_value_set_name  
FROM  
 `rax-staging-dev.operational_reporting_oracle_stage.raw_fnd_flex_values`    ffv
LEFT OUTER JOIN  
 `rax-staging-dev.operational_reporting_oracle_stage.raw_fnd_flex_value_sets`   ffvs
ON ffv.flex_value_set_id = ffvs.flex_value_set_id  
LEFT OUTER JOIN  
 `rax-staging-dev.operational_reporting_oracle_stage.raw_fnd_flex_values_tl`     ffvt
ON ffv.flex_value_id = ffvt.flex_value_id   
WHERE  
 UPPER(ffvs.flex_value_set_name) IN('RS_TEAM','RS_BUSINESS_UNITS','RS_DEPARTMENTS','RS_COMPANY', 'RS_PRODUCT','RS_LOCATION', 'RS_ACCOUNT')  
;
 
------------------------  
--GL Account Desc  
------------------------  
UPDATE    cloud_gl_account_description  as  ded  
SET    
    GL_Account_Description = seg.DESCRIPTION  
FROM      flex_values seg  
WHERE ded.GL_Code = seg.flex_value  
AND UPPER(seg.flex_value_set_name)='RS_ACCOUNT'  
AND UPPER(ded.GL_Account_Description) ='UNKNOWN' ; 
---------------------------------------------------------------  
--RAISE USING MESSAGE ='GL Account updated'; 
-------------------------------------------------------------------------------------------------------------  

CREATE OR REPLACE TEMP TABLE GL_Code_Stage_BRM AS 
SELECT DISTINCT   --INTO      #GL_Code_Stage_BRM  
    GL_Account      AS GL_Code,  
    Oracle_GL_Account_DESC  AS GL_Account_Description,  
    ''       AS GL_Account_Group,  
    ''       AS GL_Calculation_Rule_Category,  
    ''       AS GL_Global_Net_Revenue_Bucket_Desc,  
    0       AS GL_Is_Net_Revenue,  
    0       AS GL_Is_Dedicated_Revenue  
FROM  
 stage_one.raw_dedicated_inv_event_detail A  
WHERE  
   GL_Account <> '000000'  
------  
UNION   ALL
------  
SELECT DISTINCT   
    GL_Code,  
    GL_Account_Description,  
    GL_Account_Group,  
    GL_Calculation_Rule_Category,  
    GL_Global_Net_Revenue_Bucket_Desc,  
    GL_Is_Net_Revenue,  
    GL_Is_Dedicated_Revenue  
FROM  
    cloud_gl_account_description 
WHERE  
   GL_Code <> '000000'  ;
-------------------------------------------------------------------------------------------------------------------  
INSERT INTO  
 stage_two_dw.stage_dedicated_gl_codes  
 (  
 GL_Code, GL_Account_Description, GL_Account_Group, GL_Calculation_Rule_Category, GL_Global_Net_Revenue_Bucket_Desc, GL_Is_Net_Revenue, GL_Is_Dedicated_Revenue, GL_Acount_Source_System  
 )  
SELECT DISTINCT  
 GL_Code,  
 GL_Account_Description,  
 GL_Account_Group,  
 GL_Calculation_Rule_Category,  
 GL_Global_Net_Revenue_Bucket_Desc,  
 GL_Is_Net_Revenue,  
 GL_Is_Dedicated_Revenue,  
 'BRM'       AS  GL_Acount_Source_System  
FROM   GL_Code_Stage_BRM  
WHERE   
 GL_Code NOT IN  
  (  
  SELECT DISTINCT   
   GL_Code  
  FROM  
   stage_two_dw.stage_dedicated_gl_codes  
  )   ;
-------------------------------------------------------------------------------------------------------------  
UPDATE  
  `rax-staging-dev.stage_two_dw.stage_dedicated_gl_codes`
SET   
 GL_Is_Net_Revenue=1  
FROM  
 `rax-staging-dev.stage_two_dw.stage_dedicated_gl_codes` A
WHERE  
 ( LOWER(A.GL_Global_Net_Revenue_Bucket_Desc) LIKE '%net%revenue%-%')  
 AND A.GL_Is_Net_Revenue<>1 ; 
-------------------------------------------------------------------------------------------------------------  
UPDATE   stage_two_dw.stage_dedicated_gl_codes   A
SET   
 A.GL_Account_Description=B.GL_Account_Description  
FROM     GL_Code_Stage_BRM B
WHERE A.GL_Code=B.GL_Code  
and A.GL_Account_Description<>B.GL_Account_Description  
AND UPPer(B.GL_Account_Description)  Not like '%DISABLED%'  
AND UPPer(A.GL_Acount_Source_System)='BRM'  ;
END;
