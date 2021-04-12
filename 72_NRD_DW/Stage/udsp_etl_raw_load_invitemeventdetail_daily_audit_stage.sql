CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_load_invitemeventdetail_daily_audit_stage`()
BEGIN
-----------------------------------------------------------------------------------------------------------------
create or replace table stage_one.raw_invitemeventdetail_daily_audit_stage1 as
SELECT   
	cast( concat (
   cast(A.BILL_POID_ID0 as string) 
     , '--' , cast(A.ITEM_POID_ID0 as string)	
     , '--' ,  cast(B.EVENT_POID_ID0 as string)
     ,'--', ifnull(cast(C.EBI_REC_ID as string), 'X')
     ,'--', ifnull(cast(C.TAX_REC_ID as string), 'X')) as string) AS DETAIL_UNIQUE_RECORD_ID,
    A.ACCOUNT_POID_ID0,
    A.BRM_ACCOUNTNO,
    A.ACCOUNT_ID,
    A.GL_SEGMENT,
    ----------------------------------------
    A.BILL_POID_ID0,
    A.BILL_NO,
    A.BILL_START_DATE ,
    A.BILL_END_DATE ,
    A.BILL_MOD_DATE ,
    -------------------------------------------
    A.ITEM_POID_ID0,
    A.ITEM_EFFECTIVE_DATE ,
    A.ITEM_MOD_DATE ,
    A.ITEM_NAME,
    A.ITEM_STATUS,
    A.ITEM_TYPE,
    EVENT_TYPE,
    B.EVENT_POID_ID0,
    B.EVENT_START_DATE,
    B.EVENT_END_DATE,
    B.EVENT_MOD_DATE,
    SERVICE_TYPE,
    SERVICE_OBJ_TYPE,
    B.RERATE_OBJ_ID0,
    B.BATCH_ID,
    B.EVENT_NAME,
    B.EVENT_SYS_DESCR,
    B.EVENT_RUM_NAME,
    B.EVENT_CREATED_DATE,		-- new field added 2/29/2016kvc
    --------------------------------------------
    C.PRODUCT_POID_ID0,
    C.PROD_DECSRIPTION,
    C.PRODUCT_NAME,
    C.PRODUCT_CODE,
    ---------------------------------------
    C.IMPACTBAL_EVENT_OBJ_ID0,
    C.IMPACT_CATEGORY,
    C.EBI_IMPACT_TYPE,
    C.EBI_AMOUNT,
    C.EBI_QUANTITY,
    C.EBI_RATE_TAG,
    C.EBI_REC_ID,
    C.EBI_RUM_ID,
    C.rum_name,
    C.EBI_PRODUCT_OBJ_ID0,
    USAGE_RECORD_ID,
    DC_ID,
    REGION_ID,
    RES_ID,
    RES_NAME,
    C.MANAGED_FLAG,
    C.TAX_REC_ID,
    C.TAX_NAME,
    C.TAX_TYPE_ID,
    C.TAX_ELEMENT_ID,
    C.TAX_AMOUNT,
    C.TAX_RATE_PERCENT,
    C.EBI_CURRENCY_ID,	-- new field added 12.10.15jcm
    C.EBI_PRODUCT_OBJ_Type,   -- new field added 5.24.16_kvc												
    C.EBI_GL_ID,			 -- new field added 5.24.16_kvc		   
    ACTIVITY_SERVICE_TYPE,
    ACTIVITY_EVENT_TYPE,
    ACTIVITY_RECORD_ID,
    ACTIVITY_DC_ID,
    ACTIVITY_REGION,
    ACTIVITY_RESOURCE_ID,
    ACTIVITY_RESOURCE_NAME,
    ACTIVITY_ATTR1,
    ACTIVITY_ATTR2,
    ACTIVITY_ATTR3,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
    EVENT_TYPE						     AS EVENT_POID_TYPE,
    EVENT_EARNED_START_DATE,			-- new field added 5.2.19_kvc 			
    EVENT_EARNED_END_DATE,			-- new field added 5.2.19_kvc 	
    ACTIVITY_IS_BACKBILL		              AS EVENT_FASTLANE_IS_BACKBILL,-- new field added 5.2.19_kvc 	
    EBI_OFFERING_OBJ_ID0,			-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_DEAL_CODE,		-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_GRP_CODE,			-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_SUB_GRP_CODE,		-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_IS_BACKBILL,		-- new field added 5.2.19_kvc 
    0							    AS Is_Backbill     -- new field added 5.14.19_kvc 
	,ACTIVITY_ATTR4  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,ACTIVITY_ATTR5  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,ACTIVITY_ATTR6  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,Service_Obj_Id0 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,A.Bill_Created_Date --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
from
    stage_one.raw_ssis_event_audit_load_step2 b 
inner join
    stage_one.raw_ssis_impacts_audit_load_step3  c 
on  b.event_poid_id0=c.impactbal_event_obj_id0
inner join
   stage_one.raw_ssis_invoice_audit_load_step1 a  
on b.event_item_obj_id0= a.item_poid_id0;

create or replace table stage_one.raw_invitemeventdetail_daily_audit_stage as
select 
 DETAIL_UNIQUE_RECORD_ID
,ACCOUNT_POID_ID0
,BRM_ACCOUNTNO
,ACCOUNT_ID
,GL_SEGMENT
,BILL_POID_ID0
,BILL_NO
,BILL_START_DATE
,BILL_END_DATE
,BILL_MOD_DATE
,ITEM_POID_ID0
,ITEM_EFFECTIVE_DATE
,ITEM_MOD_DATE
,ITEM_NAME
,ITEM_STATUS
,SERVICE_OBJ_TYPE
,ITEM_TYPE
,EVENT_POID_ID0
,EVENT_START_DATE
,EVENT_END_DATE
,EVENT_MOD_DATE
,SERVICE_TYPE
,EVENT_TYPE
,RERATE_OBJ_ID0
,BATCH_ID
,EVENT_NAME
,EVENT_SYS_DESCR
,EVENT_RUM_NAME
,PRODUCT_POID_ID0
,case when lower(EVENT_TYPE)  LIKE '%tax%' then 'Tax'
	else PROD_DECSRIPTION
 end as PROD_DECSRIPTION
,case when lower(EVENT_TYPE)  LIKE '%tax%' then IMPACT_TYPE_DESCRIPTION
	else PRODUCT_NAME
 end as PRODUCT_NAME
,case when lower(EVENT_TYPE)  LIKE '%tax%' then IMPACT_TYPE_DESCRIPTION
	else PRODUCT_CODE
 end as PRODUCT_CODE
,IMPACTBAL_EVENT_OBJ_ID0
,IMPACT_CATEGORY
,EBI_IMPACT_TYPE
,EBI_AMOUNT
,EBI_QUANTITY
,EBI_RATE_TAG
,EBI_REC_ID
,EBI_RUM_ID
,EBI_PRODUCT_OBJ_ID0
,USAGE_RECORD_ID
,DC_ID
,REGION_ID
,RES_ID
,RES_NAME
,MANAGED_FLAG
,TAX_REC_ID
,TAX_NAME
,TAX_TYPE_ID
,TAX_ELEMENT_ID
,TAX_AMOUNT
,TAX_RATE_PERCENT
,INVOICE_SERVICEITEM_DESCR
,PROD_CATEGORY
,IMPACT_TYPE_DESCRIPTION
,QUANTITY
,UOM
,RATE
,AMOUNT
,EBI_CURRENCY_ID
,EVENT_CREATED_DATE
,EBI_PRODUCT_OBJ_Type
,EBI_GL_ID
,EVENT_FASTLANE_SERVICE_TYPE
,EVENT_FASTLANE_EVENT_TYPE
,EVENT_FASTLANE_RECORD_ID
,EVENT_FASTLANE_DC_ID
,EVENT_FASTLANE_REGION
,EVENT_FASTLANE_RESOURCE_ID
,EVENT_FASTLANE_RESOURCE_NAME
,EVENT_FASTLANE_ATTR1
,EVENT_FASTLANE_ATTR2
,EVENT_FASTLANE_ATTR3
,FASTLANE_IMPACT_CATEGORY
,FASTLANE_IMPACT_VALUE
,EVENT_POID_TYPE
,tblload_dtt
,case when ( EVENT_EARNED_START_DATE IS NULL  or EVENT_EARNED_START_DATE= '1970-01-01')
	then '1900-01-01' 
	else EVENT_EARNED_START_DATE
 end as EVENT_EARNED_START_DATE
,case when ( EVENT_EARNED_END_DATE IS NULL  or EVENT_EARNED_END_DATE= '1970-01-01')
	then  '1900-01-01' 
	else EVENT_EARNED_END_DATE
 end as EVENT_EARNED_END_DATE
,EVENT_FASTLANE_IS_BACKBILL
,EBI_OFFERING_OBJ_ID0
,FASTLANE_IMPACT_DEAL_CODE
,FASTLANE_IMPACT_GRP_CODE
,FASTLANE_IMPACT_SUB_GRP_CODE
,FASTLANE_IMPACT_IS_BACKBILL
,case when  (
			(
				(CAST(EVENT_EARNED_END_DATE as date) <> '1970-01-01')
				AND ifnull(EVENT_EARNED_END_DATE,'1970-01-01') <= ifnull(BILL_START_DATE,'1970-01-01')
				AND ifnull(Is_Backbill,0)=0
				)
			or
				(
					ifnull(FASTLANE_IMPACT_IS_BACKBILL,0)<>0 AND ifnull(Is_Backbill,0)=0
				)
			or	
				(
					ifnull(EVENT_FASTLANE_IS_BACKBILL,0)<>0 AND ifnull(Is_Backbill,0)=0
				)
			or
				(
					lower(ifnull(FASTLANE_IMPACT_SUB_GRP_CODE,'Unknown')) like  '%backbill%'  AND ifnull(Is_Backbill,0)=0
				)
			)
	then	1
	else  Is_Backbill
 end as  Is_Backbill
,ACTIVITY_ATTR4
,ACTIVITY_ATTR5
,ACTIVITY_ATTR6
,Service_Obj_Id0
,Bill_Created_Date
from(
SELECT distinct
    DETAIL_UNIQUE_RECORD_ID,
    ACCOUNT_POID_ID0,
    BRM_ACCOUNTNO,
    ACCOUNT_ID,
    GL_SEGMENT,
    ----------------------------------------
    BILL_POID_ID0,
    BILL_NO,
    BILL_START_DATE ,
    BILL_END_DATE ,
    BILL_MOD_DATE ,
    -------------------------------------------
    ITEM_POID_ID0,
    ITEM_EFFECTIVE_DATE ,
    ITEM_MOD_DATE ,
    ITEM_NAME,
    ITEM_STATUS,
    SERVICE_OBJ_TYPE,
    ITEM_TYPE,
    EVENT_POID_ID0,
    EVENT_START_DATE,
    EVENT_END_DATE,
    EVENT_MOD_DATE,
   CASE  
	   WHEN lower(EVENT_TYPE) =  '/event/activity/rax/fastlane'	THEN  ACTIVITY_SERVICE_TYPE
	   WHEN lower(SERVICE_OBJ_TYPE) = '/service/rax/fastlane/cas' and lower(EVENT_TYPE) like '/event/billing/cycle/discount'	THEN  FASTLANE_IMPACT_CATEGORY
    ELSE 
	   SERVICE_OBJ_TYPE  
    END						 AS  SERVICE_TYPE,
    EVENT_TYPE,
    RERATE_OBJ_ID0,
    BATCH_ID,
    EVENT_NAME,
    EVENT_SYS_DESCR,
    EVENT_RUM_NAME,
    --------------------------------------------
    PRODUCT_POID_ID0,
    PROD_DECSRIPTION,
    PRODUCT_NAME,
    PRODUCT_CODE,
    ---------------------------------------
    IMPACTBAL_EVENT_OBJ_ID0,
    IMPACT_CATEGORY,
    EBI_IMPACT_TYPE,
    EBI_AMOUNT,
    EBI_QUANTITY,
    EBI_RATE_TAG,
    EBI_REC_ID,
    EBI_RUM_ID,
	EBI_PRODUCT_OBJ_ID0,
    --------------------------------------
    ifnull(USAGE_RECORD_ID,ACTIVITY_RECORD_ID)	  AS USAGE_RECORD_ID, --REVISION_jcm
    ifnull(DC_ID, ACTIVITY_DC_ID)				  AS DC_ID,		  --REVISION_jcm
    ifnull( REGION_ID,ACTIVITY_REGION)			  AS REGION_ID,	  --REVISION_jcm
    ifnull( RES_ID, ACTIVITY_RESOURCE_ID)		  AS RES_ID,		  --REVISION_jcm
    ifnull( RES_NAME,ACTIVITY_RESOURCE_NAME)		  AS RES_NAME,		  --REVISION_jcm
     --------------------------------------
    MANAGED_FLAG,
    --------------------------------------
    TAX_REC_ID,
    TAX_NAME,
    TAX_TYPE_ID,
    TAX_ELEMENT_ID,
    TAX_AMOUNT,
    TAX_RATE_PERCENT,
    ifnull(SERVICE_OBJ_TYPE, ITEM_TYPE)		  AS INVOICE_SERVICEITEM_DESCR, 
    ifnull(EVENT_RUM_NAME, RUM_NAME)			  AS PROD_CATEGORY,
    CASE 
    WHEN 
		EBI_IMPACT_TYPE = 258 
    OR (EBI_IMPACT_TYPE  = 128 AND EBI_AMOUNT > 0 AND IMPACT_CATEGORY <>'MANAGED') 
    OR (EBI_IMPACT_TYPE = 1 AND upper(IMPACT_CATEGORY) like '%LEGACY_MANAGED_FEE%' )		-- new logic_jcm_12.02.15
    OR EBI_IMPACT_TYPE = 1   THEN  'CHARGE'
    WHEN EBI_IMPACT_TYPE  = 128 AND upper(IMPACT_CATEGORY) = 'MANAGED'  THEN 'MANAGED_FEE'
    WHEN EBI_IMPACT_TYPE  = 1 AND upper(IMPACT_CATEGORY) like '%LEGACY_MANAGED_FEE%' THEN  'MANAGED_FEE'	-- new logic_jcm_12.02.15
    WHEN EBI_IMPACT_TYPE = 128 AND EBI_AMOUNT <= 0 
    THEN  ( CASE
    WHEN upper(IMPACT_CATEGORY) = 'RAX_DSC'	THEN 'DISCOUNT - Racker'
    WHEN upper(IMPACT_CATEGORY) = 'DEV_DISC' THEN 'DISCOUNT - Developer'
    WHEN upper(IMPACT_CATEGORY)  like 'DEVP_DISC%'	  THEN	'DISCOUNT - Developer+'			-- new logic_jcm
    WHEN upper(IMPACT_CATEGORY) like 'INT_DSC%'    THEN 'DISCOUNT - Internal'			     --revised logic_jcm12.02.15
    WHEN upper(IMPACT_CATEGORY) = 'COMMIT'	THEN 'DISCOUNT - Commit'
    WHEN upper(IMPACT_CATEGORY) like  '%VOL_DSC'	 THEN 'DISCOUNT - Volume'
    WHEN upper(IMPACT_CATEGORY) in ('START_DISC','MAN_ST_DSC')	THEN 'DISCOUNT - Startup'
    WHEN (upper(IMPACT_CATEGORY) like 'FREE_FULL_DISC%' OR upper(IMPACT_CATEGORY) like 'FREE_MAX_DISC%' )   THEN 'DISCOUNT - Free Trial'
    WHEN upper(IMPACT_CATEGORY) = 'COMP_CYCLE'	THEN 'Compute Cycle Benefit'
    WHEN upper(IMPACT_CATEGORY) = 'BW_OUT'	THEN 'BW_OUT Benefit'
    WHEN upper(IMPACT_CATEGORY) = 'DISK_STOR'	THEN 'Disk Storage Benefit'
    WHEN upper(IMPACT_CATEGORY) = 'MSSQL_STOR'	THEN 'MSSQL Storage Benefit'
    WHEN upper(IMPACT_CATEGORY)  like 'SPL_DSC%'	 THEN 'DISCOUNT - Special'		-- new logic_jcm
    WHEN lower(EVENT_TYPE) LIKE '%_bwout' AND upper(IMPACT_CATEGORY) like '%BW_OUT'	THEN concat('Bandwidth - Tier ' , substr(IMPACT_CATEGORY, 1, 1))
    WHEN lower(EVENT_TYPE) LIKE '%_bwcdn' AND (upper(IMPACT_CATEGORY) like '%BW_CDN%'	OR  upper(IMPACT_CATEGORY)  like  '%BW_ATARI%') THEN concat('DISCOUNT - Tier ' , substr(upper(IMPACT_CATEGORY), 1, 1))
    WHEN lower(EVENT_TYPE) LIKE '%cdn_bwout' AND  (upper(IMPACT_CATEGORY)  like '%BW_CDN%'  OR  upper(IMPACT_CATEGORY)  like  '%CDBOUT') THEN concat('DISCOUNT - Tier ' , substr(IMPACT_CATEGORY, 1, 1)   ) 	-- new logic_jcm
    ELSE
	    'DISCOUNT'  
    END 
    )
    WHEN  lower(EVENT_TYPE) = '/event/billing/cycle/tax'
    THEN   (CASE TAX_TYPE_ID
						WHEN 0 THEN 'Federal Tax'
						WHEN 1 THEN 'State Tax'
						WHEN 2 THEN 'County Tax'
						WHEN 3 THEN 'Local Sales Tax'
						WHEN 8 THEN 'Local Sales Tax'
						ELSE 'Other Tax' END	  )
    END						    AS IMPACT_TYPE_DESCRIPTION,
    CASE
    WHEN IMPACT_CATEGORY  like '%MANAGED%'										THEN 1
    WHEN lower(EVENT_TYPE) IN ('/event/delayed/rax/cloud/bigdata_uptime',
    '/event/delayed/rax/cloud/database_compute',	
    '/event/delayed/rax/cloud/monitoring',
    '/event/delayed/rax/cloud/ldbal_uptime',
    '/event/delayed/rax/cloud/site_ssl_cert',
    '/event/delayed/rax/cloud/server_uptime',
    '/event/delayed/rax/cloud/legacy_server_uptime')
    THEN EBI_QUANTITY/3600
    WHEN lower(EVENT_TYPE) IN ('/event/delayed/rax/cloud/bigdata_bwout',
    '/event/delayed/rax/cloud/ldbal_bwout',
    '/event/delayed/rax/cloud/files_bwout',
    '/event/delayed/rax/cloud/files_bwcdn',
    '/event/delayed/rax/cloud/site_stor',
    '/event/delayed/rax/cloud/queue_bwout',
    '/event/delayed/rax/cloud/server_bwout',
    '/event/delayed/rax/cloud/legacy_server_bwout',
    '/event/delayed/rax/cloud/files_stor', --Added 3/12/2015
    '/event/delayed/rax/cloud/glance',
    '/event/delayed/rax/cloud/cdn_bwout')  --Added 3/12/2015
    AND upper(IMPACT_CATEGORY)  not like '%MANAGED%'
    THEN EBI_QUANTITY/1024
    WHEN lower(EVENT_TYPE) IN ('/event/delayed/rax/cloud/cbckup_license') THEN EBI_QUANTITY
    WHEN lower(EVENT_TYPE) IN ('/event/delayed/rax/cloud/database_storage',
    '/event/delayed/rax/cloud/cbs_storage',
    '/event/delayed/rax/cloud/cbs_volume') THEN EBI_QUANTITY/61440
    WHEN lower(EVENT_TYPE) IN ('/event/delayed/rax/cloud/ldbal_conn') THEN EBI_QUANTITY/100
    WHEN lower(EVENT_TYPE) IN ('/event/delayed/rax/cloud/site_met_usage')  and rum_name=  'BW_OUT'  THEN EBI_QUANTITY/1024  
    WHEN lower(EVENT_TYPE) IN ('/event/delayed/rax/cloud/site_met_usage')  and rum_name =  'COMP_CYCLE'  THEN EBI_QUANTITY 
    WHEN lower(EVENT_TYPE) IN ('/event/delayed/rax/cloud/server_ip', '/event/delayed/rax/cloud/legacy_server_ip')   THEN EBI_QUANTITY/60
    WHEN (lower(EVENT_TYPE)  like '/event/delayed/rax/cloud/database%'  OR lower(EVENT_TYPE)  like '/event/delayed/rax/cloud/server%'	
    OR lower(EVENT_TYPE) like '/event/delayed/rax/cloud/legacy_server%')	
    AND upper(IMPACT_CATEGORY) like '%MANAGED%'		
    THEN 1
    ELSE     EBI_QUANTITY						 END 	AS QUANTITY,
    (CASE 
    WHEN ( EBI_IMPACT_TYPE = 258 )
    AND lower(EVENT_TYPE) IN ('/event/delayed/rax/cloud/monitoring',
    '/event/delayed/rax/cloud/database_compute',
    '/event/delayed/rax/cloud/server_uptime',
    '/event/delayed/rax/cloud/legacy_server_uptime') THEN 'HOURS'
    WHEN (EBI_IMPACT_TYPE = 258 ) 
    AND lower(EVENT_TYPE) IN (
    '/event/delayed/rax/cloud/server_bwout',
    '/event/delayed/rax/cloud/legacy_server_bwout')  THEN 'GB'		
    WHEN (EBI_IMPACT_TYPE = 258 )
    AND lower(EVENT_TYPE) IN ('/event/delayed/rax/cloud/database_storage',
    '/event/delayed/rax/cloud/cbs_volume',
    '/event/delayed/rax/cloud/cbs_storage')         THEN 'GB_HRS'
     WHEN ( EBI_IMPACT_TYPE = 1 )
    AND lower(EVENT_TYPE) IN ('/event/billing/product/fee/purchase')
    AND SERVICE_OBJ_TYPE IN ('/service/rax/cloud/site/domain')
										   THEN 'OCCURRENCE'
    WHEN IMPACT_CATEGORY like '%MANAGED%' THEN	'OCCURRENCE'			 -- new logic_jcm
    WHEN  lower(EVENT_TYPE) = '/event/activity/Rax/fastlane'  THEN	 'SERVICE'	 -- new logic_jcm
    WHEN (EBI_IMPACT_TYPE = 258 OR EBI_IMPACT_TYPE = 1 )
     THEN 
    (CASE
    WHEN EBI_RATE_TAG LIKE '%;%'
   then SUBSTR(SUBSTR(EBI_RATE_TAG, STRPOS(EBI_RATE_TAG,'|')+1, LENGTH(EBI_RATE_TAG)),1, STRPOS(SUBSTR(EBI_RATE_TAG, STRPOS(EBI_RATE_TAG,'|')+1, LENGTH(EBI_RATE_TAG)),';')-1)
    when  EBI_RATE_TAG LIKE '%|%|%'
    then   SUBSTR(SUBSTR(EBI_RATE_TAG, STRPOS(EBI_RATE_TAG,'|')+1, LENGTH(EBI_RATE_TAG)),1, STRPOS(SUBSTR(EBI_RATE_TAG, STRPOS(EBI_RATE_TAG,'|')+1, LENGTH(EBI_RATE_TAG)),'|')-1)
    ELSE   SUBSTR(EBI_RATE_TAG, STRPOS(EBI_RATE_TAG,'|')+1, length(EBI_RATE_TAG))
    END)		
    ELSE NULL     END)					 AS UOM,
    CASE 
    WHEN IMPACT_CATEGORY	like '%MANAGED%'  THEN  cast(EBI_AMOUNT as string)		-- new logic_jcm
    WHEN SERVICE_TYPE like '/service/rax/cloud/site/domain'	THEN cast(EBI_AMOUNT as string)		
    WHEN lower(EVENT_TYPE) = '/event/billing/cycle/tax'	THEN NULL
    WHEN lower(EVENT_TYPE) = '/event/activity/Rax/fastlane' THEN '1'			-- new logic_jcm
    WHEN (EBI_IMPACT_TYPE = 1 AND lower(EVENT_TYPE) in ('/event/billing/product/fee/cycle/cycle_forward_annual','/event/billing/product/fee/purchase') )	THEN NULL
    WHEN EBI_RATE_TAG LIKE '%|%'    THEN substr(EBI_RATE_TAG,1, strpos(EBI_RATE_TAG,'|')-1)		
    END		AS  RATE,
    CASE      WHEN lower(EVENT_TYPE) = '/event/billing/cycle/tax'				--|| v_TaxEventType ||
			THEN ifnull( TAX_AMOUNT    ,  EBI_AMOUNT)		-- when tax record is not found, use ebi value_jcm09.22.16
			ELSE EBI_AMOUNT     
	END					  AS AMOUNT,
    EBI_CURRENCY_ID,	-- new field added 12.10.15jcm
    EVENT_CREATED_DATE,		-- new field added 2/29/2016kvc
    EBI_PRODUCT_OBJ_Type,   -- new field added 5.24.16_kvc												
    EBI_GL_ID,			 -- new field added 5.24.16_kvc		   
    ACTIVITY_SERVICE_TYPE				AS EVENT_FASTLANE_SERVICE_TYPE,
    ACTIVITY_EVENT_TYPE					AS EVENT_FASTLANE_EVENT_TYPE,
    ACTIVITY_RECORD_ID					AS EVENT_FASTLANE_RECORD_ID,
    ACTIVITY_DC_ID						AS EVENT_FASTLANE_DC_ID,
    ACTIVITY_REGION					    AS EVENT_FASTLANE_REGION,
    ACTIVITY_RESOURCE_ID				AS EVENT_FASTLANE_RESOURCE_ID,
    ACTIVITY_RESOURCE_NAME				AS EVENT_FASTLANE_RESOURCE_NAME,
    ACTIVITY_ATTR1						AS EVENT_FASTLANE_ATTR1,
    ACTIVITY_ATTR2						AS EVENT_FASTLANE_ATTR2,
    ACTIVITY_ATTR3						AS EVENT_FASTLANE_ATTR3,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
    EVENT_TYPE						    AS EVENT_POID_TYPE,
    current_datetime()						    AS tblload_dtt,
    EVENT_EARNED_START_DATE,			-- new field added 5.2.19_kvc 			
    EVENT_EARNED_END_DATE,			-- new field added 5.2.19_kvc 	
    EVENT_FASTLANE_IS_BACKBILL,		-- new field added 5.2.19_kvc 	
    EBI_OFFERING_OBJ_ID0,			-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_DEAL_CODE,		-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_GRP_CODE,			-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_SUB_GRP_CODE,		-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_IS_BACKBILL,		-- new field added 5.2.19_kvc 
    Is_Backbill					-- new field added 5.14.19_kvc 
	,ACTIVITY_ATTR4  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,ACTIVITY_ATTR5  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,ACTIVITY_ATTR6  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,Service_Obj_Id0 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,Bill_Created_Date --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging 
FROM stage_one.raw_invitemeventdetail_daily_audit_stage1
);


UPDATE  stage_one.raw_invitemeventdetail_daily_audit_stage
SET
   EBI_PRODUCT_OBJ_ID0=B.Product_ID,
   PRODUCT_POID_ID0=B.Product_ID
FROM
     stage_one.raw_invitemeventdetail_daily_audit_stage A
INNER JOIN
   stage_two_dw.stage_cloud_hosting_products B
ON (A.Product_Name = B.Product_Name and a.PRODUCT_POID_ID0=b.product_id)
WHERE
    lower(A.EVENT_TYPE)  LIKE '%tax%' 
AND lower(Product_ID_NK) like '%brm_%'
;

END;
