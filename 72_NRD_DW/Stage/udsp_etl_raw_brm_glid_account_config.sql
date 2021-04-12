CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_brm_glid_account_config`()
BEGIN

 declare glsub_1  int64 default 3  ;
declare glsub_2  int64 default 3  ;
declare glsub_3  int64 default 6  ;
declare glsub_4  int64 default  3 ;
declare glsub_5  int64 default 4  ;
declare glsub_6  int64 default  4 ;
declare glsub_7  int64 default  4 ;

create or replace temp table Raw_BRM_GLID_ACCOUNT_CONFIG as
SELECT  --INTO      #Raw_BRM_GLID_ACCOUNT_CONFIG  
    Glid_Configuration_Nk  AS Raw_BRM_GLID_ACCOUNT_CONFIG_PK,  
    config_poid,  
    config_type,  
    glid_obj_id0,  
    glid_rec_id,  
    glid_descr,  
    glid_taxcode,  
    GLSeg_poid,  
    GLSeg_type,  
    GLSeg_value,  
    GLSeg_descr,  
    GLSeg_name,  
    GLAcct_obj_id0,  
    GLAcct_rec_id,  
    GLAcct_rec_id2,  
    GLAcct_attribute,  
    GLAcct_record_type,  
    GLAcct_offset_acct,  
    substring(GLAcct_offset_acct,1,(select glsub_1))  as glsub1_company,  
    substring(GLAcct_offset_acct,(glsub_1)+2,(glsub_2)) as glsub2_locationdc,  
    substring(GLAcct_offset_acct,(select glsub_1+1+glsub_2)+2,  glsub_3) as glsub3_acct_subprod,  
    substring(GLAcct_offset_acct,(select glsub_1+1+glsub_2+1+glsub_3+2), glsub_4)   as glsub4_team,  
    substring(GLAcct_offset_acct,(select glsub_1+1+glsub_2+1+glsub_3+1+glsub_4)+2,glsub_5) as glsub5_busunit,  
    substring(GLAcct_offset_acct,(select glsub_1+1+glsub_2+1+glsub_3+1+glsub_4+1+glsub_5+2),glsub_6)  as glsub6_dept,  
    substring(GLAcct_offset_acct,(select glsub_1+1+glsub_2+1+glsub_3+1+glsub_4+1+glsub_5+1+glsub_6+2) ,glsub_7 )  as glsub7_product,  
    substring(GLAcct_offset_acct,(select glsub_1+1+glsub_2+1+glsub_3+1+glsub_4+1+glsub_5+1+glsub_6+1+glsub_7+2),60 ) as glsub8_unkn,  
    GLAcct_ar_acct,  
    current_datetime()      as etl_dtt   
FROM   (
		SELECT  
			GLID_PK,  
			Glid_Configuration_Nk,  
			config_poid,  
			config_type,  
			glid_obj_id0,  
			glid_rec_id,  
			glid_descr,  
			glid_taxcode,  
			GLSeg_poid,  
			GLSeg_type,  
			GLSeg_value,  
			GLSeg_descr,  
			GLSeg_name,  
			GLAcct_obj_id0,  
			GLAcct_rec_id,  
			GLAcct_rec_id2,  
			GLAcct_attribute,  
			GLAcct_record_type,  
			GLAcct_offset_acct,  
			GLAcct_ar_acct  
		from(	
				SELECT   --INTO  #GLID_ACCOUNT_CONFIG  
					CAST( concat(CAST(config_poid as string),'-',CAST(glid_rec_id as string),'-', CAST(GLSeg_name as string))as string)     AS GLID_PK,  
					concat(CAST(glid_rec_id as string),'-',CAST(GLSeg_name as string),'-',CAST(GLAcct_record_type as string),'-',CAST(GLAcct_attribute as string))  AS Glid_Configuration_Nk,     *  
				FROM ( 
				select   
					cfg.poid_id0     as config_poid,  
					cfg.poid_type     as config_type,  
					glid.obj_id0     as glid_obj_id0,  
					glid.rec_id     as glid_rec_id,  
					glid.descr      as glid_descr,  
					glid.tax_code     as glid_taxcode,  
					CFGseg.poid_id0     as GLSeg_poid,  
					CFGseg.poid_type    as GLSeg_type,  
					CFGseg.value     as GLSeg_value,  
					CFGseg.descr     as GLSeg_descr,  
					CFGseg.name     as GLSeg_name,  
					glact.obj_id0     as GLAcct_obj_id0,  
					glact.rec_id     as GLAcct_rec_id,  
					glact.rec_id2     as GLAcct_rec_id2,  
					glact.attribute     as GLAcct_attribute,  
					glact.type     as GLAcct_record_type,  
					glact.GL_OFFSET_ACCT    as GLAcct_offset_acct,  
					glact.GL_AR_ACCT    as GLAcct_ar_acct  
				FROM   
					`rax-landing-qa`.brm_ods.config_t cfg 
				INNER JOIN  -- isolate to only /glid config type records  
					`rax-landing-qa`.brm_ods.config_glid_t glid    
				on glid.obj_id0 = cfg.poid_id0  
				INNER JOIN  -- get segment level secondary configuration details  
					`rax-landing-qa`.brm_ods.config_t AS CFGseg    
				ON  lower(CFGseg.poid_type) ='/config/gl_segment'  
				AND lower(CFGseg.VALUE) =concat('0.0.0.1 /config/glid ', cast(cfg.POID_ID0 as string), ' 0' )
				INNER JOIN  -- get account based gl segmentation strings  
				   `rax-landing-qa`.brm_ods.config_glid_accts_t glact  
				ON glid.OBJ_ID0 = glact.OBJ_ID0  
				AND glid.REC_ID = glact.REC_ID2   
				AND glact.ATTRIBUTE = 1  
				AND glact.TYPE = (8)  
				WHERE   
					 ( lower(CFGseg.name) like '%cloud%' OR lower(CFGseg.name) like '%dedicated%' OR lower(CFGseg.name) like '%email%'))  
				UNION ALL
					 
				SELECT   --INTO       #GLID_ACCOUNT_CONFIG_TYPE  
					 CAST(concat(CAST(config_poid as string),'-',CAST(glid_rec_id as string),'-', CAST(GLSeg_name as string))as string)     AS GLID_PK
					,CAST(concat(CAST(glid_rec_id as string),'-',CAST(GLSeg_name as string),'-',CAST(GLAcct_record_type as string),'-',CAST(GLAcct_attribute as string)) as string)  AS Glid_Configuration_Nk    
					,*  
				FROM   
					(
				select   
					cfg.poid_id0     as config_poid,  
					cfg.poid_type     as config_type,  
					glid.obj_id0     as glid_obj_id0,  
					glid.rec_id     as glid_rec_id,  
					glid.descr      as glid_descr,  
					glid.tax_code     as glid_taxcode,  
					CFGseg.poid_id0     as GLSeg_poid,  
					CFGseg.poid_type    as GLSeg_type,  
					CFGseg.value     as GLSeg_value,  
					CFGseg.descr     as GLSeg_descr,  
					CFGseg.name     as GLSeg_name,  
					glact.obj_id0     as GLAcct_obj_id0,  
					glact.rec_id     as GLAcct_rec_id,  
					glact.rec_id2     as GLAcct_rec_id2,  
					glact.attribute     as GLAcct_attribute,  
					glact.type     as GLAcct_record_type,  
					glact.GL_OFFSET_ACCT    as GLAcct_offset_acct,  
					glact.GL_AR_ACCT    as GLAcct_ar_acct  
				FROM   
					`rax-landing-qa`.brm_ods.config_t cfg    
				INNER JOIN  -- isolate to only /glid config type records  
					`rax-landing-qa`.brm_ods.config_glid_t glid    
				on glid.obj_id0 = cfg.poid_id0  
				INNER JOIN  -- get segment level secondary configuration details  
					`rax-landing-qa`.brm_ods.config_t AS CFGseg     
				ON    lower(CFGseg.poid_type) ='/config/gl_segment'  
				AND   lower(CFGseg.VALUE) = concat('0.0.0.1 /config/glid ' , cast(cfg.POID_ID0 as string), ' 0') 
				INNER JOIN  -- get account based gl segmentation strings  
				   `rax-landing-qa`.brm_ods.config_glid_accts_t glact    
				ON glid.OBJ_ID0 = glact.OBJ_ID0  
				AND glid.REC_ID = glact.REC_ID2   
				AND glact.ATTRIBUTE = 1  
				AND glact.TYPE = 2  
				WHERE   
					 ( lower(CFGseg.name) like '%cloud%' OR  lower(CFGseg.name) like '%dedicated%' OR  lower(CFGseg.name) like '%email%')
				)
		)
)--#GLID_ACCOUNT_CONFIG  
;

CREATE OR REPLACE TEMP TABLE New_GLID_ACCOUNT_CONFIG AS 
SELECT DISTINCT   --INTO     #New_GLID_ACCOUNT_CONFIG  
  CAST(concat(CAST(glid_rec_id AS STRING),'-', '.dedicated','-',CAST(GLAcct_record_type as STRING),'-',CAST(GLAcct_attribute as STRING)) AS STRING) AS PK
 , 34977926912 AS config_poid
 , config_type
 , 34977926912 AS glid_obj_id0
 , glid_rec_id
 , glid_descr
 , glid_taxcode
 , 34977923200 AS GLSeg_poid
 , GLSeg_type
 , '0.0.0.1 /config/glid 34977923200 0' AS GLSeg_value
 , GLSeg_descr
 , '.dedicated' AS GLSeg_name
 , 34977923200 AS GLAcct_obj_id0
 , GLAcct_rec_id
 , GLAcct_rec_id2
 , GLAcct_attribute
 , GLAcct_record_type
 , '000.123.414500.000.0000.0000.8001.0000' GLAcct_offset_acct
 , '000' AS glsub1_company
 , glsub2_locationdc
 , glsub3_acct_subprod
 , glsub4_team
 , glsub5_busunit
 , glsub6_dept
 , glsub7_product
 , glsub8_unkn
 , '000.123.120000.000.0000.0000.8001.0000' AS GLAcct_ar_acct  
FROM    
    stage_one.raw_brm_glid_account_config GL    
WHERE   
    glid_rec_id=11360108  
AND GL.GLAcct_record_type=8 -- gl for BILLED events only  
AND GL.GLAcct_attribute= 1  -- gl for net amounts only  

UNION ALL

SELECT DISTINCT   
  CAST(CONCAT(CAST(glid_rec_id AS STRING),'-', '.dedicated','-',CAST(GLAcct_record_type as STRING),'-',CAST(GLAcct_attribute as STRING)) AS STRING) AS PK
 ,34977926912 AS config_poid
 ,config_type
 ,34977926912 AS glid_obj_id0
 ,glid_rec_id
 ,glid_descr
 ,glid_taxcode
 ,34977923200 AS GLSeg_poid
 ,GLSeg_type
 ,'0.0.0.1 /config/glid 34977923200 0' AS GLSeg_value
 ,GLSeg_descr
 ,'.dedicated' AS GLSeg_name
 ,34977923200 AS GLAcct_obj_id0
 ,GLAcct_rec_id
 ,GLAcct_rec_id2
 ,GLAcct_attribute
 ,GLAcct_record_type
 ,'000.123.416100.000.0000.0000.8002.0000' GLAcct_offset_acct
 ,'000' AS glsub1_company
 ,glsub2_locationdc
 ,glsub3_acct_subprod
 ,glsub4_team
 ,glsub5_busunit
 ,glsub6_dept
 ,glsub7_product
 ,glsub8_unkn
 ,'000.123.120000.000.0000.0000.8002.0000' AS GLAcct_ar_acct  
FROM    
    stage_one.raw_brm_glid_account_config GL    
WHERE   
    glid_rec_id=11370308  
AND GL.GLAcct_record_type=8 -- gl for BILLED events only  
AND GL.GLAcct_attribute= 1  -- gl for net amounts only  

UNION ALL
SELECT DISTINCT   
  CAST(concat(CAST(glid_rec_id AS STRING),'-', '.dedicated','-',CAST(GLAcct_record_type as STRING),'-',CAST(GLAcct_attribute as STRING)) AS STRING) AS PK
 ,34977926912 AS config_poid
 ,config_type
 ,34977926912 AS glid_obj_id0
 ,glid_rec_id
 ,glid_descr
 ,glid_taxcode
 ,34977923200 AS GLSeg_poid
 ,GLSeg_type
 ,'0.0.0.1 /config/glid 34977923200 0' AS GLSeg_value
 ,GLSeg_descr
 ,'.dedicated' AS GLSeg_name
 ,34977923200 AS GLAcct_obj_id0
 ,GLAcct_rec_id
 ,GLAcct_rec_id2
 ,GLAcct_attribute
 ,GLAcct_record_type
 ,'000.123.414500.000.0000.0000.8002.0000' GLAcct_offset_acct
 ,'000' AS glsub1_company
 ,glsub2_locationdc
 ,glsub3_acct_subprod
 ,glsub4_team
 ,glsub5_busunit
 ,glsub6_dept
 ,glsub7_product
 ,glsub8_unkn
 ,'000.123.120000.000.0000.0000.8002.0000' AS GLAcct_ar_acct  
FROM    
    stage_one.raw_brm_glid_account_config GL    
WHERE   
    glid_rec_id=11370108  
AND GL.GLAcct_record_type=8 -- gl for BILLED events only  
AND GL.GLAcct_attribute= 1  -- gl for net amounts only  

UNION ALL
SELECT DISTINCT   
 CAST(concat(CAST(glid_rec_id AS STRING),'-', '.dedicated','-',CAST(GLAcct_record_type as STRING),'-',CAST(GLAcct_attribute as STRING)) AS STRING) AS PK
 ,34977926912 AS config_poid
 ,config_type
 ,34977926912 AS glid_obj_id0
 ,glid_rec_id
 ,glid_descr
 ,glid_taxcode
 ,34977923200 AS GLSeg_poid
 ,GLSeg_type
 ,'0.0.0.1 /config/glid 34977923200 0' AS GLSeg_value
 ,GLSeg_descr
 ,'.dedicated' AS GLSeg_name
 ,34977923200 AS GLAcct_obj_id0
 ,GLAcct_rec_id
 ,GLAcct_rec_id2
 ,GLAcct_attribute
 ,GLAcct_record_type
 ,'000.123.416100.000.0000.0000.8001.0000' GLAcct_offset_acct
 ,'000' AS glsub1_company
 ,glsub2_locationdc
 ,glsub3_acct_subprod
 ,glsub4_team
 ,glsub5_busunit
 ,glsub6_dept
 ,glsub7_product
 ,glsub8_unkn
 ,'000.123.120000.000.0000.0000.8001.0000' AS GLAcct_ar_acct  
FROM    
    stage_one.raw_brm_glid_account_config GL    
WHERE   
    glid_rec_id=11360308  
AND GL.GLAcct_record_type=8 -- gl for BILLED events only  
AND GL.GLAcct_attribute= 1  -- gl for net amounts only  

union all
SELECT DISTINCT   
  CAST(concat(CAST(glid_rec_id AS string),'-', '.dedicated','-',CAST(GLAcct_record_type as string),'-',CAST(GLAcct_attribute as string)) AS string) AS PK
 ,34977926912 AS config_poid
 ,config_type
 ,34977926912 AS glid_obj_id0
 ,glid_rec_id
 ,glid_descr
 ,glid_taxcode
 ,34977923200 AS GLSeg_poid
 ,GLSeg_type
 ,'0.0.0.1 /config/glid 34977923200 0' AS GLSeg_value
 ,GLSeg_descr
 ,'.dedicated' AS GLSeg_name
 ,34977923200 AS GLAcct_obj_id0
 ,GLAcct_rec_id
 ,GLAcct_rec_id2
 ,GLAcct_attribute
 ,GLAcct_record_type
 ,'000.123.416100.000.0000.0000.8003.0000' GLAcct_offset_acct
 ,'000' AS glsub1_company
 ,glsub2_locationdc
 ,glsub3_acct_subprod
 ,glsub4_team
 ,glsub5_busunit
 ,glsub6_dept
 ,glsub7_product
 ,glsub8_unkn
 ,'000.123.120000.000.0000.0000.8003.0000' AS GLAcct_ar_acct  
FROM    
    stage_one.raw_brm_glid_account_config GL    
WHERE   
    glid_rec_id=11380308  
AND GL.GLAcct_record_type=8 -- gl for BILLED events only  
AND GL.GLAcct_attribute= 1  -- gl for net amounts only  

union all
select
CAST(concat(CAST(0 AS string),'-', '.dedicated','-',CAST(2 as string),'-',CAST(1 as string)) AS string) AS PK
 ,0 AS config_poid
 ,'N/A' AS config_type
 ,0 AS glid_obj_id0
 ,0 AS glid_rec_id
 ,'N/A' AS glid_descr
 ,'N/A' glid_taxcode
 ,0 AS GLSeg_poid
 ,'N/A' AS GLSeg_type
 ,'N/A' AS GLSeg_value
 ,null GLSeg_descr
 ,'.dedicated' AS GLSeg_name
 ,0 AS GLAcct_obj_id0
 ,0 AS GLAcct_rec_id
 ,0 GLAcct_rec_id2
 ,1 AS GLAcct_attribute
 ,2 AS GLAcct_record_type
 ,'N/A' GLAcct_offset_acct
 ,'000' AS glsub1_company
 ,'000' AS glsub2_locationdc
 ,'000' AS glsub3_acct_subprod
 ,'000' AS glsub4_team
 ,'000' AS glsub5_busunit
 ,'000' AS glsub6_dept
 ,'000' AS glsub7_product
 ,'000' AS glsub8_unkn
 ,'000' AS GLAcct_ar_acct  
 
 union all
 select
 CAST(concat(CAST(glid_rec_id AS string),'-', '.cloud.us','-',CAST(GLAcct_record_type as string),'-',CAST(GLAcct_attribute as string)) AS string) AS PK
 ,35072324379 AS config_poid
 ,config_type
 ,35072324379 AS glid_obj_id0
 ,90076 AS glid_rec_id
 ,'Data Migration Opening Balance Adjustment' glid_descr
 ,'GST' AS glid_taxcode
 ,35072326427 AS GLSeg_poid
 ,GLSeg_type
 ,'0.0.0.1 /config/glid 35072324379 0' AS GLSeg_value
 ,GLSeg_descr
 ,'.cloud.us' AS GLSeg_name
 ,35072324379 AS GLAcct_obj_id0
 ,21 AS GLAcct_rec_id
 ,GLAcct_rec_id2
 ,GLAcct_attribute
 ,GLAcct_record_type
 ,'002.000.120000.000.0000.0000.0000.0000' GLAcct_offset_acct
 ,'002' AS glsub1_company
 ,glsub2_locationdc
 ,'120000' AS glsub3_acct_subprod
 ,glsub4_team
 ,glsub5_busunit
 ,glsub6_dept
 ,glsub7_product
 ,glsub8_unkn
 ,'002.000.120000.000.0000.0000.0000.0000' AS GLAcct_ar_acct   
FROM        stage_one.raw_brm_glid_account_config GL    
WHERE   
    glid_rec_id=90076  
AND GL.GLAcct_record_type=8 -- gl for BILLED events only  
AND GL.GLAcct_attribute= 1  -- gl for net amounts only  

UNION ALL 
SELECT 
 CAST(CONCAT(CAST(0 AS STRING),'-', '.cloud.us','-',CAST(2 as STRING),'-',CAST(1 as STRING)) AS STRING) AS PK
 ,0 AS config_poid
 ,'N/A' AS config_type
 ,0 AS glid_obj_id0
 ,0 AS glid_rec_id
 ,'N/A' AS glid_descr
 ,'N/A' glid_taxcode
 ,0 AS GLSeg_poid
 ,'N/A' AS GLSeg_type
 ,'N/A' AS GLSeg_value
 ,null GLSeg_descr
 ,'.cloud.us' AS GLSeg_name
 ,0 AS GLAcct_obj_id0
 ,0 AS GLAcct_rec_id
 ,0 GLAcct_rec_id2
 ,1 AS GLAcct_attribute
 ,2 AS GLAcct_record_type
 ,'N/A' GLAcct_offset_acct
 ,'000' AS glsub1_company
 ,'000' AS glsub2_locationdc
 ,'000' AS glsub3_acct_subprod
 ,'000' AS glsub4_team
 ,'000' AS glsub5_busunit
 ,'000' AS glsub6_dept
 ,'000' AS glsub7_product
 ,'000' AS glsub8_unkn
 ,'000' AS GLAcct_ar_acct;
 
INSERT INTO  
    Raw_BRM_GLID_ACCOUNT_CONFIG  
SELECT distinct  
   PK, config_poid, config_type, glid_obj_id0, glid_rec_id, glid_descr, glid_taxcode, GLSeg_poid, GLSeg_type, GLSeg_value, GLSeg_descr, GLSeg_name, GLAcct_obj_id0, GLAcct_rec_id, GLAcct_rec_id2, GLAcct_attribute, GLAcct_record_type, GLAcct_offset_acct, glsub1_company, glsub2_locationdc, glsub3_acct_subprod, glsub4_team, glsub5_busunit, glsub6_dept, glsub7_product, glsub8_unkn, GLAcct_ar_acct, CURRENT_DATETIME() AS etl_dtt  
FROM  
   New_GLID_ACCOUNT_CONFIG   
WHERE  
  PK NOT IN(SELECT Raw_BRM_GLID_ACCOUNT_CONFIG_PK FROM Raw_BRM_GLID_ACCOUNT_CONFIG) ;   
---------------------------------------------------------------------------------------------------------------  

--------------------------------------------------------------------------------------------------------------------  
CREATE OR REPLACE TABLE stage_one.raw_brm_glid_account_config AS 
SELECT  
    Raw_BRM_GLID_ACCOUNT_CONFIG_PK, config_poid, config_type, glid_obj_id0, glid_rec_id, glid_descr, glid_taxcode, GLSeg_poid, GLSeg_type, GLSeg_value, GLSeg_descr, GLSeg_name, GLAcct_obj_id0, GLAcct_rec_id, GLAcct_rec_id2, GLAcct_attribute, GLAcct_record_type, GLAcct_offset_acct, glsub1_company, glsub2_locationdc, glsub3_acct_subprod, glsub4_team, glsub5_busunit, glsub6_dept, glsub7_product, glsub8_unkn, GLAcct_ar_acct, etl_dtt  
FROM  
    Raw_BRM_GLID_ACCOUNT_CONFIG  ;

END;
