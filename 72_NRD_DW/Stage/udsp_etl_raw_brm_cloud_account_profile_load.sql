CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_brm_cloud_account_profile_load`()
BEGIN

-- =============================================  
-- Author:  jcmcconnell  
-- Create date: 10.11.2016  
-- Description: consolidate all BRM account and account profile data to a single reporting table  
-- =============================================  
/*Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				rama4760			27/05/2019	 copied from 72 server as part of NRD	  
*/ 
-----------------------------------------------------------------------------------------------------------------------  
  
---------------------------------------------------------------------------------------------------------------------  
create or replace temp table RAW_BRM_Cloud_ACCOUNT_PROFILE as 
SELECT  --INTO     #RAW_BRM_Cloud_ACCOUNT_PROFILE  
    PROFILE_POID,  
    POID_TYPE         AS PROFILE_TYPE,  
    ACCOUNT_OBJ_ID0,  
    ACCOUNT_POID,  
    ACCOUNT_NO         AS BRM_ACCOUNT_NO,  
    substr(account_no, strpos(account_no,'-')+1,64)  AS ACCOUNT_ID,    
    ifnull(CORE_ACCT_NO,substr(ACCOUNT_NO, strpos(ACCOUNT_NO,'-')+1,64)) AS ACCOUNT_NUMBER,  
    CAST('0' AS string)      AS BDOM,  
    CORE_ACCT_NO,  
    LINE_OF_BUSINESS,  
    ifnull(COMPANY,EXT_ACCOUNT_NAME)     AS COMPANY_NAME,  
    COUNTRY,  
    GL_SEGMENT,  
    STATUS,  
    ADDRESS,  
    CITY,  
    CONTACT_TYPE,  
    EMAIL_ADDR,  
    FIRST_NAME,  
    LAST_NAME,  
    MIDDLE_NAME,  
    SALUTATION,  
    STATE,  
    TITLE,  
    ZIP,  
    COUNTY,  
    CONTACT_PRIMARY,  
    GEOCODE,  
    CURRENCY,  
    LOCALE,  
    STATUS          AS ACCOUNT_STATUS,  
    BUSINESS_TYPE,  
    CONFIG_BUSINESS_TYPE       AS BUSINESS_TYPE_DESCR,  
    NAME           AS profile_name,  
    ACCOUNT_TYPE,  
    CUSTOMER_TYPE,  
    CUSTOMER_TYPE_DESCR,  
    BILLING_SEGMENT,  
    CONFIG_BILLING_SEGMENT       AS BILLING_SEG_DESCR,  
    CONTRACTING_ENTITY,  
    SUPPORT_TEAM,  
    STRING          AS SUPPORT_TEAM_DESCR,  
    team_customer_type,  
    team_cust_class,  
    substr(ORG, strpos(ORG,' ')+1, 2) AS ORG_VALUE,  
    ORG         AS ORGANIZATION,  
    BUSINESS_UNIT,  
    PAYMENT_TERM,
	  cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(PO_END_DATE  as int64) second)  as datetime) AS PO_END_DATE,  
    cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(PO_START_DATE  as int64) second)  as datetime) AS PO_START_DATE,  
    cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(effective_t  as int64) second)  as datetime) AS profile_effective_dtt,  
    cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(CONSOLIDATION_T  as int64) second)  as datetime) AS Consolidation_date,  
    CASE   
    MANUAL_ALLOCATE  
 WHEN   
    1 THEN 'MANUAL'  
 ELSE   
    'AUTO'  
 END               AS MANUAL_ALLOCATE,  
 ifnull( INVOICE_CONSOLIDATION_FLAG,0)    AS Is_Consolidation_Account,  
 CASE   
    ifnull(INVOICE_CONSOLIDATION_FLAG,0)  
 WHEN   
    1  
 THEN   
    CONSOLIDATION_ACCOUNT  
 ELSE   
    ''  
 END           AS Consolidation_Account,  
    profile_dwloaddtt,  
    rax_profile_dwloaddtt  

FROM (
select   
    prf.POID_ID0         AS PROFILE_POID,  
    prf.POID_TYPE,  
    prf.ACCOUNT_OBJ_ID0,  
    act.poid_id0         AS ACCOUNT_POID,  
    ACCOUNT_NO,  
    rax_prf.CORE_ACCT_NO,  
    LINE_OF_BUSINESS,  
    COMPANY,  
    EXT_ACCOUNT_NAME,  
    COUNTRY,  
    GL_SEGMENT,  
    CURRENCY,  
    STATUS,  
    ADDRESS,  
    CITY,  
    CONTACT_TYPE,  
    FIRST_NAME,  
    LAST_NAME,  
    MIDDLE_NAME,  
    EMAIL_ADDR,  
    SALUTATION,  
    STATE,  
    TITLE,  
    ZIP,  
    COUNTY,  
    CONTACT_PRIMARY,  
    LOCALE,  
    GEOCODE,  
    PROVINCE,  
    BUSINESS_TYPE,  
    bus.CONFIG_BUSINESS_TYPE,  
    prf.NAME,  
    rax_prf.ACCOUNT_TYPE,  
    rax_prf.CUSTOMER_TYPE,  
    cust.DESCR         AS CUSTOMER_TYPE_DESCR,  
    cust.BILLING_SEGMENT,  
    seg.CONFIG_BILLING_SEGMENT,  
    rax_prf.CONTRACTING_ENTITY,  
    rax_prf.SUPPORT_TEAM,  
    team.STRING,  
    team.CUSTOMER_TYPE        AS team_customer_type,  
    team.DESCR         AS team_cust_class,  
    rax_prf.ORG,  
    rax_prf.BUSINESS_UNIT,  
    rax_prf.PAYMENT_TERM,  
    rax_prf.PO_END_DATE,  
    rax_prf.PO_START_DATE,  
    prf.effective_t,  
    MANUAL_ALLOCATE,  
    rax_prf.CONSOLIDATION_T,  
    INVOICE_CONSOLIDATION_FLAG,  
    INVOICE_CONSOLIDATION_ACCOUNT     AS CONSOLIDATION_ACCOUNT,  
    prf.dw_timestamp        AS profile_dwloaddtt,  
    rax_prf.dw_timestamp       AS rax_profile_dwloaddtt  
FROM  
   `rax-landing-qa`.brm_ods.account_t act    
LEFT OUTER JOIN  
    `rax-landing-qa`.brm_ods.profile_t prf    
on act.poid_id0=prf.account_obj_id0  
LEFT OUTER JOIN  
    `rax-landing-qa`.brm_ods.profile_rackspace_t  rax_prf    
on prf.poid_id0 = rax_prf.obj_id0  
LEFT OUTER JOIN  
   `rax-landing-qa`.brm_ods.config_profile_cust_type_t cust    
on rax_prf.CUSTOMER_TYPE=cust.REC_ID  /*cust.CUSTOMER_TYPE,*/ -- all type values in profile = 0_jcm??  
 --  and cust.BILLING_SEGMENT, = 2004  -- dedicated  
LEFT OUTER JOIN  
    `rax-landing-qa`.brm_ods.config_billing_segment_t seg    
on cust.BILLING_SEGMENT=seg.REC_ID  
LEFT OUTER JOIN  
    `rax-landing-qa`.brm_ods.config_business_type_t bus    
on act.BUSINESS_TYPE=bus.REC_ID  
LEFT OUTER JOIN  
    `rax-landing-qa`.brm_ods.config_sup_team_col_prof_t team    
on rax_prf.SUPPORT_TEAM=team.SUPPORT_TEAM  
LEFT OUTER JOIN  
    `rax-landing-qa`.brm_ods.account_nameinfo_t nam  
on act.poid_id0=nam.obj_id0  
where     
    lower(act.GL_SEGMENT) like '%.cloud%'
AND lower(prf.poid_type) = '/profile/rackspace' 
AND upper(nam.CONTACT_TYPE) IN('PRIMARY_CONTACT','BILLING') ) ;

UPDATE   RAW_BRM_Cloud_ACCOUNT_PROFILE  A 
SET  
    BDOM=cast(ACTG_CYCLE_DOM  as string)
FROM   
(  
SELECT  
    ACCOUNT_OBJ_ID0,  
    ACTG_CYCLE_DOM   
FROM   
    ( 
SELECT DISTINCT   
    ACCOUNT_OBJ_ID0,  
    ACTG_CYCLE_DOM   
FROM  
   `rax-landing-qa`.brm_ods.billinfo_t B   
)) B  
where A.ACCOUNT_POID = B.ACCOUNT_OBJ_ID0  
;
---------------------------------------------------------------------------------------------------------------------  
UPDATE   RAW_BRM_Cloud_ACCOUNT_PROFILE  A
SET  
    ACCOUNT_NUMBER='911164'   
WHERE   
    ACCOUNT_NUMBER like '%911164-%'  ;
---------------------------------------------------------------------------------------------------------------------  
UPDATE   RAW_BRM_Cloud_ACCOUNT_PROFILE  A   
SET  
   LINE_OF_BUSINESS ='UK_CLOUD'  
WHERE   
   ACCOUNT_NUMBER >='10000000'  
AND LINE_OF_BUSINESS is null ; 
---------------------------------------------------------------------------------------------------------------------  

	
UPDATE RAW_BRM_Cloud_ACCOUNT_PROFILE  A   
SET  
    A.FIRST_NAME=B.FIRST_NAME,  
    A.LAST_NAME=B.LAST_NAME,   
    A.MIDDLE_NAME=B.MIDDLE_NAME,    
    A.EMAIL_ADDR=B.EMAIL_ADDR,    
    A.SALUTATION=B.SALUTATION   
FROM   
    (
		SELECT  --into     #PRIMARY_CONTACT  
			ACCOUNT_NUMBER,  
			FIRST_NAME,  
			LAST_NAME,   
			MIDDLE_NAME,    
			EMAIL_ADDR,    
			SALUTATION   
		from  RAW_BRM_Cloud_ACCOUNT_PROFILE   
		where  
			upper(CONTACT_TYPE) ='PRIMARY_CONTACT' 
	) B --#PRIMARY_CONTACT  B   
WHERE A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER  
and   
    upper(A.CONTACT_TYPE) ='BILLING'  
AND A.FIRST_NAME is null  ;


UPDATE  RAW_BRM_Cloud_ACCOUNT_PROFILE  A
SET  
 A.COMPANY_NAME=B.COMPANY_NAME  
FROM RAW_BRM_Cloud_ACCOUNT_PROFILE  B   
WHERE A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER  
and upper(A.CONTACT_TYPE) ='PRIMARY_CONTACT'  
AND A.COMPANY_NAME is null  
AND B.COMPANY_NAME is not null ;


create or replace table  stage_one.raw_brm_cloud_account_profile  as
SELECT DISTINCT  
    PROFILE_POID,  
    PROFILE_TYPE,  
    ACCOUNT_POID,  
    A.ACCOUNT_OBJ_ID0,  
    BRM_ACCOUNT_NO,  
    ACCOUNT_ID,  
    ACCOUNT_NUMBER,  
    BDOM,  
    CORE_ACCT_NO,  
    CASE   
		WHEN upper(RACKER_INTERNAL) ='RACKER' THEN   1  
		ELSE 0  
    END        AS Is_Racker_Account,  
    CASE   
		WHEN upper(RACKER_INTERNAL) ='INTERNAL' THEN   1  
		ELSE 0  
    END        AS Is_Internal_Account,  
    RACKER_INTERNAL,  
    parent_account_poid    AS child_account_poid,  
    child_account_no    AS parent_account_no,  
    child_account_poid    AS parent_account_poid,  
    LINE_OF_BUSINESS,  
    COMPANY_NAME,  
    CONTACT_TYPE,  
    GL_SEGMENT,  
    STATUS,  
    ADDRESS,  
    CITY,  
    EMAIL_ADDR,  
    FIRST_NAME,  
    LAST_NAME,  
    MIDDLE_NAME,  
    SALUTATION,  
    STATE,  
    TITLE,  
    ZIP,  
    COUNTY,  
    CONTACT_PRIMARY,  
    COUNTRY,  
    GEOCODE,  
    CURRENCY,  
    LOCALE,  
    ACCOUNT_STATUS,  
    BUSINESS_TYPE,  
    BUSINESS_TYPE_DESCR,  
    profile_name,  
    ACCOUNT_TYPE,  
    CUSTOMER_TYPE,  
    CUSTOMER_TYPE_DESCR,  
    BILLING_SEGMENT,  
    BILLING_SEG_DESCR,  
    CONTRACTING_ENTITY,  
    SUPPORT_TEAM,  
    SUPPORT_TEAM_DESCR,  
    team_customer_type,  
    team_cust_class,  
    ORG_VALUE,  
    ORGANIZATION,  
    BUSINESS_UNIT,  
    PAYMENT_TERM,  
    PO_END_DATE,  
    PO_START_DATE,  
    profile_effective_dtt,  
    MANUAL_ALLOCATE     AS Manual_Allocate,  
    CASE   
		WHEN child_account_no like '%030%' THEN Consolidation_date  
		ELSE Null  
    END        AS Consolidation_date,  
    '-1'       AS Consolidation_Account,   
    CASE   
		WHEN child_account_no like '%030%' THEN 1  
		ELSE 0  
    END        AS Is_Consolidation_Account,  
    profile_dwloaddtt,  
    rax_profile_dwloaddtt,  
    CURRENT_DATETIME()      AS table_etl_loaddtt  

FROM  
 RAW_BRM_Cloud_ACCOUNT_PROFILE    A  
LEFT OUTER JOIN   
   (
		SELECT  --INTO      #Parent_info  
		 parent_account_poid,  
		 child_account_no,  
		 child_account_poid  
		FROM (
		select   
		 pb.account_obj_id0 as parent_account_poid,  
		 ac.account_no  as child_account_no,   
		 ac.poid_id0      AS child_account_poid  
		from   
		(   select distinct   
			parent_billinfo_obj_id0,   
			account_obj_id0  
			from    
			`rax-landing-qa`.brm_ods.billinfo_t    
		) as  PB   
		inner join  
			`rax-landing-qa`.brm_ods.billinfo_t  as BI       
		on pb.parent_billinfo_obj_id0 = bi.poid_id0   
		inner join  
			`rax-landing-qa`.brm_ods.account_t as  ac     
		on ac.poid_id0 = bi.account_obj_id0    
		where      
			bi.account_obj_id0 <> pb.account_obj_id0)
   ) PRAN --#Parent_info PRAN  
ON A.ACCOUNT_POID =PRAN.parent_account_poid  
LEFT OUTER JOIN  
    (
		SELECT DISTINCT   --INTO    #RACKER_INTERNAL  
			account_obj_id0,  
			RACKER_INTERNAL  
		FROM  
			(
				SELECT DISTINCT   --INTO     #RACKER_INTERNAL_Stage  
					account_obj_id0,  
					NAME,    
				CASE   
				  WHEN lower(NAME) Like '%racker%'   THEN 'RACKER'  
				  WHEN lower(NAME) Like '%internal%' THEN 'INTERNAL'  
				ELSE   
					''  
					END AS RACKER_INTERNAL  
				FROM  
				(  
				SELECT  
					account_obj_id0,  
					NAME  
				FROM   (
				SELECT  
					PP.account_obj_id0,  
					P.NAME  
				FROM   
					`rax-landing-qa`.brm_ods.product_t P   
				INNER JOIN   
					`rax-landing-qa`.brm_ods.purchased_product_t PP   
				ON PP.PRODUCT_OBJ_ID0 = P.POID_ID0  
				INNER JOIN   
					`rax-landing-qa`.brm_ods.account_t A   
				ON A.POID_ID0 = PP.ACCOUNT_OBJ_ID0  
				WHERE   
					A.ACCOUNT_NO LIKE '020-%'  
				AND PP.STATUS = 1  
				AND PP.STATUS = 1   
				AND (
				   lower(P.NAME) LIKE '%racker%'  
				OR lower(P.NAME) LIKE '%internal%') 
				)  
				------  
				UNION  all
				------  
				SELECT  
					ACCOUNT_OBJ_ID0,  
					NAME  
				FROM  (
				SELECT  
					PD.ACCOUNT_OBJ_ID0,  
					D.NAME  
				FROM  
					`rax-landing-qa`.brm_ods.discount_t D   
				INNER JOIN   
					`rax-landing-qa`.brm_ods.purchased_discount_t PD   
				ON PD.DISCOUNT_OBJ_ID0 = D.POID_ID0  
				INNER JOIN  
					`rax-landing-qa`.brm_ods.account_t A   
				ON A.POID_ID0 = PD.ACCOUNT_OBJ_ID0  
				WHERE   
					A.ACCOUNT_NO LIKE '021-%'  
				AND PD.STATUS = 1  
				AND (
				   lower(D.NAME) LIKE '%racker%'  
				OR lower(D.NAME) LIKE '%internal%'
				) )  
				) A
			)--#RACKER_INTERNAL_Stage 
	) rack--#RACKER_INTERNAL rack  
ON A.ACCOUNT_POID=rack.account_obj_id0  ;


--------------------------------------------------------------------------------------------------------------- -- data duplicate issues
UPDATE     stage_one.raw_brm_cloud_account_profile  A   
SET  A.Consolidation_Account= cast(B.ACCOUNT_NUMBER as string) 
FROM   stage_one.raw_brm_dedicated_account_profile  B   
where A.parent_account_poid=B.ACCOUNT_POID  and A.CONTACT_TYPE=B.CONTACT_TYPE;
---------------------------------------------------------------------------------------------------------------  
      
END;
