CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_stage_dedicated_gl_products_brm`()
BEGIN

/*Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				rama4760			31/05/2019	 copied from 72 server as part of NRD	  
*/ 
------------------------------------------------------------------------------------------------------------------------  

create or replace temp table Cloud_products as
SELECT DISTINCT --INTO      #Cloud_products    
    GL.glsub7_product    AS GL_Product_Code,  
    cast('' as string)   AS GL_Product_Description,  
    CAST('Cloud'as string)  AS GL_Product_Hierarchy,  
    'BRM'       AS GL_Product_Source_System,  
    'BRM'       AS GL_Product_Billing_Source 
FROM  
    stage_one.raw_invitemeventdetail_daily_stage A
left outer join  
    stage_one.raw_brm_glid_account_config GL  
    ON A.EBI_GL_ID= glid_rec_id  
inner join   
    stage_two_dw.stage_brm_glid_acct_atttribute atr  
on GL.GLAcct_attribute= atr.Attribute_id  
inner join   
    stage_two_dw.stage_brm_glid_acct_report_type typ  
ON GL.GLAcct_record_type=typ.Report_Type_id  
AND A.GL_SEGMENT= GL.GLSeg_name   
AND GL.GLAcct_record_type IN (2,8) -- gl for BILLED events only  
AND GL.GLAcct_attribute= 1   
WHERE  
      GL.glsub7_product NOT IN  
  (  
  SELECT DISTINCT   
   GL_Product_Code  
  FROM  
   stage_two_dw.stage_gl_products  
  )  
  
union all  
SELECT DISTINCT   
    GL.glsub7_product    AS GL_Product_Code,  
    cast(''as string)   AS GL_Product_Description,  
    CAST('Dedicated'as string) AS GL_Product_Hierarchy,  
    'BRM'       AS GL_Product_Source_System,  
    'BRM'       AS GL_Product_Billing_Source  
FROM  
   stage_one.raw_dedicated_inv_event_detail A   
left outer join  
    stage_one.raw_brm_glid_account_config GL   
    ON A.glid_rec_id= GL.glid_rec_id  
inner join   
    stage_two_dw.stage_brm_glid_acct_atttribute atr  
on GL.GLAcct_attribute= atr.Attribute_id  
inner join   
    stage_two_dw.stage_brm_glid_acct_report_type typ  
ON GL.GLAcct_record_type=typ.Report_Type_id  

AND A.GL_SEGMENT= GL.GLSeg_name   
AND GL.GLAcct_record_type IN (2,8) -- gl for BILLED events only  
AND GL.GLAcct_attribute= 1   
WHERE  
      GL.glsub7_product NOT IN  
  (  
  SELECT DISTINCT   
   GL_Product_Code  
  FROM  
   stage_two_dw.stage_gl_products  
  );
 
 
/** BRM GL SEGMENT DETAILS  **/  
/**  Oracle GL Values  ***/  
create or replace temp table flex_values as
SELECT  --INTO    #flex_values   
    flex_value,-- used in update for Oracle ID  
    DESCRIPTION,-- used in update for Oracle description  
    flex_value_set_name  

FROM (
SELECT   
    flex_value,  
    ffvt.DESCRIPTION,  
    flex_value_set_name  
FROM  
 `rax-landing-qa`.operational_reporting_oracle.raw_fnd_flex_values    ffv    
LEFT OUTER JOIN  
 `rax-landing-qa`.operational_reporting_oracle.raw_fnd_flex_value_sets   ffvs    
ON ffv.flex_value_set_id = ffvs.flex_value_set_id  
LEFT OUTER JOIN  
 `rax-landing-qa`.operational_reporting_oracle.raw_fnd_flex_values_tl     ffvt     
ON ffv.flex_value_id = ffvt.flex_value_id   
WHERE  
 upper(ffvs.flex_value_set_name) = 'RS_PRODUCT'  
)  ;


-------------------------------------------------------------------------------------------------------------------  
--PRODUCT  
-------------------------------------------------------------------------------------------------------------------  
UPDATE   Cloud_products    ded  
SET    
    GL_Product_Description = seg.DESCRIPTION  
FROM  flex_values seg  
WHERE ded.GL_Product_Code = seg.flex_value  
AND upper(seg.flex_value_set_name)='RS_PRODUCT'  
AND upper(ded.GL_Product_Description) = 'UNKNOWN'  ;
-------------------------------------------------------------------------------------------------------------  

INSERT INTO   stage_two_dw.stage_gl_products  
 (  
 GL_Product_Code, GL_Product_Description, GL_Product_Hierarchy, GL_Product_BU,GL_Product_BU_Name, GL_Product_Internal_BU, GL_Product_Internal_BU_Name, GL_Product_Source_System, GL_Product_Billing_Source  
 )  
SELECT  DISTINCT  
    GL_Product_Code,  
    GL_Product_Description,  
    GL_Product_Hierarchy,  
    GL_Product_BU,  
    GL_Product_BU_Name,  
    GL_Product_Internal_BU,  
    GL_Product_Internal_BU_Name,  
    GL_Product_Source_System,  
    GL_Product_Billing_Source  
FROM   
 (
		 SELECT DISTINCT   --INTO  #GL_Products_BRM  
			GL_Product_Code,  
			GL_Product_Description,  
			GL_Product_Hierarchy,  
			IFNULL(PRODUCT_BU,'Unknown')      AS GL_Product_BU,  
			IFNULL(PRODUCT_BU_NAME,'Unknown')     AS GL_Product_BU_Name,  
			IFNULL(INTERNAL_BU,'Unknown')      AS GL_Product_Internal_BU,  
			IFNULL(INTERNAL_BU_NAME,'Unknown')     AS GL_Product_Internal_BU_Name,  
			GL_Product_Source_System,  
			GL_Product_Billing_Source  
		FROM  
			Cloud_products A   
		LEFT OUTER JOIN  
			(
				SELECT DISTINCT *  --#XREF_PRODUCT_BU  
				FROM (
				SELECT   
					PRODUCT_NUMBER, 
					PRODUCT_DESCRIPTION, 
					Pres_Layer AS INTERNAL_BU, 
					Pres_Layer_Name AS INTERNAL_BU_NAME, 
					Parent_PROD_BU AS PRODUCT_BU,     
					Parent_PROD_BU_Name    AS PRODUCT_BU_NAME  
				FROM   
					`rax-landing-qa`.r_accounting.fin_prod_bu 
					)
			) B--#XREF_PRODUCT_BU B  
		ON A.GL_Product_Code=B.PRODUCT_NUMBER
 )--#GL_Products_BRM  
WHERE   
 GL_Product_Code NOT IN  
  (  
  SELECT DISTINCT   
   GL_Product_Code  
  FROM  
   stage_two_dw.stage_gl_products
  )  ;
-------------------------------------------------------------------------------------------------------------------  
UPDATE  stage_two_dw.stage_gl_products A  
SET   
 A.GL_Product_Description=B.DESCRIPTION  
FROM    flex_values B   
WHERE A.GL_Product_Code=B.flex_value  
AND UPPER(GL_Product_Billing_Source)<>'EBS'  
AND UPPER(RTRIM(A.GL_Product_Description))<>UPPER(LTRIM(B.DESCRIPTION))   
AND (UPPER(B.DESCRIPTION)  Not like '%DISABLED%'  
AND  UPPER(B.DESCRIPTION)  Not like '%UNKNOWN%') ; 
-------------------------------------------------------------------------------------------------------------------  
UPDATE   stage_two_dw.stage_gl_products  A
SET   
 A.GL_Product_BU=PRODUCT_BU,  
 A.GL_Product_BU_Name=PRODUCT_BU_Name,  
 A.GL_Product_Internal_BU=INTERNAL_BU,  
 A.GL_Product_Internal_BU_Name=INTERNAL_BU_Name   
FROM   (
		SELECT DISTINCT *  --#XREF_PRODUCT_BU  
		FROM (
		SELECT   
			PRODUCT_NUMBER, 
			PRODUCT_DESCRIPTION, 
			Pres_Layer AS INTERNAL_BU, 
			Pres_Layer_Name AS INTERNAL_BU_NAME, 
			Parent_PROD_BU AS PRODUCT_BU,     
			Parent_PROD_BU_Name    AS PRODUCT_BU_NAME  
		FROM   
			`rax-landing-qa`.r_accounting.fin_prod_bu 
			)
	) B--XREF_PRODUCT_BU B   
WHERE A.GL_Product_Code=B.PRODUCT_NUMBER  
AND  
     (IFNULL(UPPER(A.GL_Product_BU),'UNKNOWN')<>B.PRODUCT_BU  
OR    IFNULL(UPPER(A.GL_Product_BU_Name),'UNKNOWN')<>B.PRODUCT_BU_Name  
OR    IFNULL(UPPER(A.GL_Product_Internal_BU),'UNKNOWN')<>B.INTERNAL_BU  
OR    IFNULL(UPPER(A.GL_Product_Internal_BU_Name),'UNKNOWN')<>B.INTERNAL_BU_Name  
 ) ; 


END;
