CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_brm_dedicated_account_profile_load`()
BEGIN

/*Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				anil2912			07/06/2019	 copied from 72 server as part of NRD	  
*/  
--------------------------------------------------------------------------------  
-----------------------------------------------------------------------------------------------------------------------

create or replace temp table BRM_DEDICATED_ACCOUNT_PROFILE as 
SELECT --INTO    #BRM_DEDICATED_ACCOUNT_PROFILE
    PROFILE_POID,
    POID_TYPE									AS PROFILE_TYPE,
    ACCOUNT_OBJ_ID0,
    ACCOUNT_POID,
    REC_ID,
    ACCOUNT_NO									AS BRM_ACCOUNT_NO,
    SUBSTR(account_no, STRPOS(account_no,'-')+1,64)	AS ACCOUNT_ID,  
    IFNULL(CORE_ACCT_NO,SUBSTR(ACCOUNT_NO, STRPOS(ACCOUNT_NO,'-')+1,64)) AS ACCOUNT_NUMBER,
	CAST('0' AS STRING)      AS BDOM,
    CORE_ACCT_NO,
    LINE_OF_BUSINESS,
    IFNULL(COMPANY,EXT_ACCOUNT_NAME)					AS COMPANY_NAME,
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
    STATUS							   AS ACCOUNT_STATUS,
    BUSINESS_TYPE,
    CONFIG_BUSINESS_TYPE				   AS BUSINESS_TYPE_DESCR,
    NAME								   AS profile_name,
    ACCOUNT_TYPE,
    CUSTOMER_TYPE,
    CUSTOMER_TYPE_DESCR,
    BILLING_SEGMENT,
    CONFIG_BILLING_SEGMENT				   AS BILLING_SEG_DESCR,
    CONTRACTING_ENTITY,
    SUPPORT_TEAM,
    STRING							   AS SUPPORT_TEAM_DESCR,
    team_customer_type,
    team_cust_class,
    SUBSTR(ORG, STRPOS(ORG,' ')+1, 2)	   AS ORG_VALUE,
    ORG								   AS ORGANIZATION,
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
	END							        AS MANUAL_ALLOCATE,
	IFNULL( INVOICE_CONSOLIDATION_FLAG,0)	   AS Is_Consolidation_Account,
	CASE 
	   IFNULL(INVOICE_CONSOLIDATION_FLAG,0)
	WHEN 
	   1
	THEN 
	   CONSOLIDATION_ACCOUNT
	ELSE 
	   ''
	END								   AS Consolidation_Account,
	profile_dwloaddtt,
     rax_profile_dwloaddtt

FROM (
select 
    prf.POID_ID0								 AS PROFILE_POID,
    prf.POID_TYPE,
    prf.ACCOUNT_OBJ_ID0,
    nam.REC_ID,
    act.poid_id0								 AS ACCOUNT_POID,
    ACCOUNT_NO,
    EXT_ACCOUNT_NAME,                                             
    rax_prf.CORE_ACCT_NO,
    rax_prf.LINE_OF_BUSINESS,
    COMPANY,
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
    cust.DESCR								 AS CUSTOMER_TYPE_DESCR,
    cust.BILLING_SEGMENT,
    seg.CONFIG_BILLING_SEGMENT,
    rax_prf.CONTRACTING_ENTITY,
    rax_prf.SUPPORT_TEAM,
    team.STRING,
    team.CUSTOMER_TYPE							 AS team_customer_type,
    team.DESCR								 AS team_cust_class,
    rax_prf.ORG,
    rax_prf.BUSINESS_UNIT,
    rax_prf.PAYMENT_TERM,
    rax_prf.PO_END_DATE,
    rax_prf.PO_START_DATE,
    prf.effective_t,
    MANUAL_ALLOCATE,
    rax_prf.CONSOLIDATION_T,
    INVOICE_CONSOLIDATION_FLAG,
    INVOICE_CONSOLIDATION_ACCOUNT				 AS CONSOLIDATION_ACCOUNT,
    prf.dw_timestamp						 AS profile_dwloaddtt,
    rax_prf.dw_timestamp						 AS rax_profile_dwloaddtt
from
   `rax-landing-qa`.brm_ods.account_t act  
left outer join
    `rax-landing-qa`.brm_ods.profile_t prf  
on act.poid_id0=prf.account_obj_id0
left outer join
    `rax-landing-qa`.brm_ods.profile_rackspace_t  rax_prf  
on prf.poid_id0 = rax_prf.obj_id0
left outer join
  `rax-landing-qa`.brm_ods.config_profile_cust_type_t cust  
on rax_prf.customer_type=cust.rec_id  /*cust.customer_type,*/ -- all type values in profile = 0_jcm??
 --  and cust.billing_segment, = 2004  -- dedicated
left outer join
    `rax-landing-qa`.brm_ods.config_billing_segment_t seg  
on cust.billing_segment=seg.rec_id
left outer join
    `rax-landing-qa`.brm_ods.config_business_type_t bus  
on act.business_type=bus.rec_id
left outer join
    `rax-landing-qa`.brm_ods.config_sup_team_col_prof_t team  
on rax_prf.support_team=team.support_team
left outer join
    `rax-landing-qa`.brm_ods.account_nameinfo_t nam 
on act.poid_id0=nam.obj_id0
where   
    account_no like '030%'  -- dedicated only
AND LOWER(prf.poid_type) = '/profile/rackspace'
--AND nam.COMPANY not like ''Dummy%''
AND UPPER(nam.CONTACT_TYPE) IN('PRIMARY_CONTACT','BILLING') );



UPDATE BRM_DEDICATED_ACCOUNT_PROFILE  A
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

--00:00:05
UPDATE BRM_DEDICATED_ACCOUNT_PROFILE A
SET
    ACCOUNT_NUMBER=bq_functions.udf_stripnonnumeric(ACCOUNT_NUMBER)
WHERE 
     if(SAFE_CAST(ACCOUNT_NUMBER AS FLOAT64) is null,'FALSE', 'TRUE') <> 'FALSE' --ISNUMERIC(ACCOUNT_NUMBER)<>1
AND  length(bq_functions.udf_stripnonnumeric(ACCOUNT_NUMBER)) >3
AND ACCOUNT_NUMBER not like '%-%';
---------------------------------------------------------------------------------------------------------------------
UPDATE BRM_DEDICATED_ACCOUNT_PROFILE  A 
SET
    ACCOUNT_NUMBER='911164'
WHERE 
    ACCOUNT_NUMBER like '%911164-%';
---------------------------------------------------------------------------------------------------------------------
--/* data duplicate
UPDATE BRM_DEDICATED_ACCOUNT_PROFILE A
SET
	A.FIRST_NAME=B.FIRST_NAME,
    A.LAST_NAME=B.LAST_NAME,	
    A.MIDDLE_NAME=B.MIDDLE_NAME,		
    A.EMAIL_ADDR=B.EMAIL_ADDR,		
    A.SALUTATION=B.SALUTATION	
FROM 
    (
	SELECT --into     #PRIMARY_CONTACT
    distinct ACCOUNT_NUMBER,BRM_ACCOUNT_NO,
	COMPANY_NAME,
    FIRST_NAME,
    LAST_NAME,	
    MIDDLE_NAME,		
    EMAIL_ADDR,		
    SALUTATION	
	from BRM_DEDICATED_ACCOUNT_PROFILE 
	where    upper(CONTACT_TYPE) ='PRIMARY_CONTACT'
	) B--#PRIMARY_CONTACT  B 
WHERE A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER AND A.BRM_ACCOUNT_NO=B.BRM_ACCOUNT_NO
and 
    upper(A.CONTACT_TYPE) ='BILLING'
AND A.FIRST_NAME is null;

--------------------------------------------------------------------------------------------------------------------
--/* data duplicate
UPDATE BRM_DEDICATED_ACCOUNT_PROFILE A
SET
	A.COMPANY_NAME=B.COMPANY_NAME
FROM 
    BRM_DEDICATED_ACCOUNT_PROFILE  B 
WHERE A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER AND A.BRM_ACCOUNT_NO=B.BRM_ACCOUNT_NO
and 
    upper(A.CONTACT_TYPE) ='PRIMARY_CONTACT'
AND A.COMPANY_NAME is null
AND B.COMPANY_NAME is not null;

--------------------------------------------------------------------------------------------------------------------

UPDATE BRM_DEDICATED_ACCOUNT_PROFILE  A 
SET
   ACCOUNT_NUMBER=ACCOUNT_ID
WHERE
    lower(LINE_OF_BUSINESS)='datapipe';
---------------------------------------------------------------------------------------------------------------------
UPDATE BRM_DEDICATED_ACCOUNT_PROFILE  A 
SET
   LINE_OF_BUSINESS='DEDICATED'
WHERE
     lower(LINE_OF_BUSINESS)='dedicated';

create or replace table stage_one.raw_brm_dedicated_account_profile	 as 
SELECT 
    PROFILE_POID,
    PROFILE_TYPE,
    A.ACCOUNT_POID,
    A.ACCOUNT_OBJ_ID0,
    BRM_ACCOUNT_NO,
    ACCOUNT_ID,
    ACCOUNT_NUMBER,
    BDOM,
	CORE_ACCT_NO,
	
    CASE 
	   WHEN upper(RACKER_INTERNAL) ='RACKER' THEN   1
	  
	   ELSE 0
    END 					  AS Is_Racker_Account,
    CASE 
	   WHEN upper(RACKER_INTERNAL) ='INTERNAL' THEN   1
	  
	   ELSE 0
    END 					  AS Is_Internal_Account,
    parent_account_poid,
    child_account_no,
    LINE_OF_BUSINESS,
    COMPANY_NAME,
    CONTACT_TYPE,
    GL_SEGMENT,
    STATUS,
    ADDRESS,
    CITY,
    EMAIL_ADDR,
    PHONE,
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
    MANUAL_ALLOCATE			  AS Manual_Allocate,
    Consolidation_date,
    Consolidation_Account,
    Is_Consolidation_Account,
    profile_dwloaddtt,
    rax_profile_dwloaddtt,
    current_datetime()			     AS table_etl_loaddtt
FROM
	BRM_DEDICATED_ACCOUNT_PROFILE    A
LEFT OUTER JOIN
    (
			SELECT --INTO    #Parent_info
				parent_account_poid,
				child_account_no	
			FROM (
			select 
				pb.account_obj_id0	as parent_account_poid,
				ac.account_no		as child_account_no	
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
				`rax-landing-qa`.brm_ods.account_t	as  ac   
			on ac.poid_id0 = bi.account_obj_id0  
			where   	
				bi.account_obj_id0 <> pb.account_obj_id0 )
	) PAR_ACCT --#Parent_info PAR_ACCT
ON A.ACCOUNT_POID=Par_Acct.parent_account_poid	
LEFT OUTER JOIN
    (
		SELECT --INTO	#Phone
			ACCOUNT_POID,
			string_agg(PHONE,',') Phone
		FROM 
			(
				SELECT  --#Phone_Stage
					ACCOUNT_POID,
					cast((SELECT concat(Replace(Replace(PHONE,chr(CAST(0x0020 as int64)), ''),chr(CAST(0x001A as int64)), '') , chr(10))) as string) AS PHONE
				FROM
					BRM_DEDICATED_ACCOUNT_PROFILE A
				INNER JOIN
					(
					SELECT
						OBJ_ID0, 
						PHONE,
						TYPE
					FROM
						`rax-landing-qa`.brm_ods.account_phones_t
					--WHERE
						--TYPE=2 
						)B
				ON A.ACCOUNT_POID=B.OBJ_ID0
				order by
					ACCOUNT_POID
			) x --#Phone_Stage AS x
		GROUP BY 
			ACCOUNT_POID
	) phone --#Phone phone
ON A.ACCOUNT_POID=phone.ACCOUNT_POID	
LEFT OUTER JOIN
    (
		SELECT DISTINCT --INTO    #RACKER_Dedicated_INTERNAL
			account_obj_id0,
			RACKER_INTERNAL
		FROM
			(
			SELECT DISTINCT --INTO   #RACKER_Dedicated_INTERNAL_Stage
			account_obj_id0,
			NAME,  
		CASE 
		  WHEN lower(NAME) Like '%racker%' THEN 'RACKER'
		  WHEN lower(NAME) Like '%internal%' THEN 'INTERNAL'
		ELSE 
			''
			END AS RACKER_INTERNAL

		FROM
		(
		SELECT
			account_obj_id0,
			NAME
		FROM  (
		SELECT
			PP.account_obj_id0,
			P.NAME
		from 
			`rax-landing-qa`.brm_ods.product_t p 
		inner join 
			`rax-landing-qa`.brm_ods.purchased_product_t pp 
		on pp.product_obj_id0 = p.poid_id0
		inner join 
			`rax-landing-qa`.brm_ods.account_t a 
		on a.poid_id0 = pp.account_obj_id0
		WHERE 
			A.ACCOUNT_NO LIKE '030%'
		AND PP.STATUS = 1
		AND PP.STATUS = 1 
		AND (lower(P.NAME) LIKE '%racker%'
		OR   lower(P.NAME) LIKE '%internal%') )
		------
		UNION all
		------
		SELECT
			ACCOUNT_OBJ_ID0,
			NAME
		FROM (
		SELECT
			PD.ACCOUNT_OBJ_ID0,
			D.NAME
		from
			`rax-landing-qa`.brm_ods.discount_t d 
		inner join 
		   `rax-landing-qa`.brm_ods.purchased_discount_t pd 
		on pd.discount_obj_id0 = d.poid_id0
		inner join
			`rax-landing-qa`.brm_ods.account_t a 
		on a.poid_id0 = pd.account_obj_id0
		WHERE 
			A.ACCOUNT_NO LIKE '030%'
		AND PD.STATUS = 1
		and (d.name like '%racker%'
		or d.name like '%internal%') )
		) A
			)--#RACKER_Dedicated_INTERNAL_Stage
	) rack--#RACKER_Dedicated_INTERNAL rack
ON A.ACCOUNT_POID=rack.account_obj_id0;


END;
