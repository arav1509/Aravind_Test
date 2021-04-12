CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_load_raw_invitemeventdetail_daily_stage`()
BEGIN


	------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE table stage_one.raw_invitemeventdetail_daily_stage as
SELECT
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
  SERVICE_TYPE,
-----------------------------------------------------------
    --EVENT_TYPE,--REVISION _jcm
--------------------------------------------------------
-- NEW LOGIC TO ADDRESS ACTIVITY RECORDS: --REVISION _jcm
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
USAGE_RECORD_ID, --REVISION_jcm
DC_ID,		  --REVISION_jcm
REGION_ID,	  --REVISION_jcm
RES_ID,		  --REVISION_jcm
RES_NAME,		  --REVISION_jcm
     --------------------------------------
MANAGED_FLAG,
    --------------------------------------
TAX_REC_ID,
TAX_NAME,
TAX_TYPE_ID,
TAX_ELEMENT_ID,
TAX_AMOUNT,
TAX_RATE_PERCENT,
INVOICE_SERVICEITEM_DESCR, 
PROD_CATEGORY,
   IMPACT_TYPE_DESCRIPTION,
    QUANTITY,
    UOM,
    RATE,
    AMOUNT,
EBI_CURRENCY_ID,	-- new field added 12.10.15jcm
    EVENT_CREATED_DATE,		-- new field added 2/29/2016kvc
EBI_PRODUCT_OBJ_Type,   -- new field added 5.24.16_kvc												
EBI_GL_ID,			 -- new field added 5.24.16_kvc		   
  CURRENT_DATE() as tblload_dtt,
 EVENT_FASTLANE_SERVICE_TYPE,
EVENT_FASTLANE_EVENT_TYPE,
EVENT_FASTLANE_RECORD_ID,
EVENT_FASTLANE_DC_ID,
EVENT_FASTLANE_REGION,
EVENT_FASTLANE_RESOURCE_ID,
EVENT_FASTLANE_RESOURCE_NAME,
EVENT_FASTLANE_ATTR1,
EVENT_FASTLANE_ATTR2,
EVENT_FASTLANE_ATTR3,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
EVENT_POID_TYPE,
    EVENT_EARNED_START_DATE,			-- new field added 5.2.19_kvc 			
    EVENT_EARNED_END_DATE,			-- new field added 5.2.19_kvc 	
   EVENT_FASTLANE_IS_BACKBILL,-- new field added 5.2.19_kvc 		
    EBI_OFFERING_OBJ_ID0,			-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_DEAL_CODE,		-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_GRP_CODE,			-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_SUB_GRP_CODE,		-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_IS_BACKBILL,-- new field added 5.2.19_kvc
    0							    AS Is_Backbill ,
	ACTIVITY_ATTR4, -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
ACTIVITY_ATTR5, -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
ACTIVITY_ATTR6,	-- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
Service_Obj_Id0, -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
Bill_Created_Date  --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
FROM (
SELECT distinct
  CONCAT(cast(A.BILL_POID_ID0 as STRING) , '--' 
  ,cast(A.ITEM_POID_ID0 as STRING)	
   , '--' ,  
    cast(B.EVENT_POID_ID0 as STRING)
    ,'--',
    IFNULL(cast(C.EBI_REC_ID as STRING), 'x')
     ,'--', IFNULL(cast(C.TAX_REC_ID as STRING), 'x')) 		   
     AS DETAIL_UNIQUE_RECORD_ID,
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
    A.SERVICE_OBJ_TYPE,
    A.ITEM_TYPE,
    B.EVENT_POID_ID0,
    B.EVENT_START_DATE,
    B.EVENT_END_DATE,
    B.EVENT_MOD_DATE,
   CASE  
	   WHEN LOWER(EVENT_TYPE) =  '/event/activity/rax/fastlane'	THEN  ACTIVITY_SERVICE_TYPE
	   WHEN LOWER(SERVICE_OBJ_TYPE) = '/service/rax/fastlane/cas' and LOWER(EVENT_TYPE) like '/event/billing/cycle/discount'	THEN  FASTLANE_IMPACT_CATEGORY
    ELSE 
	   SERVICE_OBJ_TYPE  
    END						 AS  SERVICE_TYPE,
-----------------------------------------------------------
    --EVENT_TYPE,--REVISION _jcm
--------------------------------------------------------
-- NEW LOGIC TO ADDRESS ACTIVITY RECORDS: --REVISION _jcm
    EVENT_TYPE,
    B.RERATE_OBJ_ID0,
    B.BATCH_ID,
    B.EVENT_NAME,
    B.EVENT_SYS_DESCR,
    B.EVENT_RUM_NAME,
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
    C.EBI_PRODUCT_OBJ_ID0,
    --------------------------------------
    IFNULL(USAGE_RECORD_ID,ACTIVITY_RECORD_ID)	  AS USAGE_RECORD_ID, --REVISION_jcm
    IFNULL(DC_ID, ACTIVITY_DC_ID)				  AS DC_ID,		  --REVISION_jcm
    IFNULL( REGION_ID,ACTIVITY_REGION)			  AS REGION_ID,	  --REVISION_jcm
    IFNULL( RES_ID, ACTIVITY_RESOURCE_ID)		  AS RES_ID,		  --REVISION_jcm
    IFNULL( RES_NAME,ACTIVITY_RESOURCE_NAME)		  AS RES_NAME,		  --REVISION_jcm
     --------------------------------------
    C.MANAGED_FLAG,
    --------------------------------------
    C.TAX_REC_ID,
    C.TAX_NAME,
    C.TAX_TYPE_ID,
    C.TAX_ELEMENT_ID,
    C.TAX_AMOUNT,
    C.TAX_RATE_PERCENT,
    IFNULL(A.SERVICE_OBJ_TYPE,  A.ITEM_TYPE)		  AS INVOICE_SERVICEITEM_DESCR, 
    IFNULL(B.EVENT_RUM_NAME, C.RUM_NAME)			  AS PROD_CATEGORY,
    CASE 
    WHEN 
    C.EBI_IMPACT_TYPE = 258 
    OR (EBI_IMPACT_TYPE  = 128 AND C.EBI_AMOUNT > 0 AND LOWER(C.IMPACT_CATEGORY) <>'managed') 
    OR (EBI_IMPACT_TYPE = 1 AND LOWER(IMPACT_CATEGORY) like '%legacy_managed_fee%' )		-- new logic_jcm_12.02.15
    OR EBI_IMPACT_TYPE = 1   THEN  'charge'
    WHEN C.EBI_IMPACT_TYPE  = 128 AND LOWER(C.IMPACT_CATEGORY) = 'managed'  THEN 'managed_fee'
    WHEN EBI_IMPACT_TYPE  = 1 AND LOWER(IMPACT_CATEGORY) like '%legacy_managed_fee%' THEN  'managed_fee'	-- new logic_jcm_12.02.15
    WHEN EBI_IMPACT_TYPE = 128 AND EBI_AMOUNT <= 0 
    THEN  (CASE
    WHEN 
    LOWER(C.IMPACT_CATEGORY) = 'rax_dsc'	THEN 'discount - racker'
    WHEN LOWER(IMPACT_CATEGORY) = 'dev_disc' THEN 'discount - developer'
    WHEN LOWER(IMPACT_CATEGORY)  like 'devp_disc%'	  THEN	'discount - developer+'			-- new logic_jcm
    WHEN LOWER(IMPACT_CATEGORY) like 'int_dsc%'    THEN 'discount - internal'			     --revised logic_jcm12.02.15
    WHEN LOWER(IMPACT_CATEGORY) = 'commit'	THEN 'discount - commit'
    WHEN LOWER(IMPACT_CATEGORY) like  '%vol_dsc'	 THEN 'discount - volume'
    WHEN LOWER(IMPACT_CATEGORY) in ('start_disc','man_st_dsc')	THEN 'discount - startup'
    WHEN (LOWER(IMPACT_CATEGORY) like 'free_full_disc%' OR LOWER(impact_category) like 'free_max_disc%' )   THEN 'discount - free trial'
    WHEN LOWER(IMPACT_CATEGORY) = 'comp_cycle'	THEN 'compute cycle benefit'
    WHEN LOWER(IMPACT_CATEGORY) = 'bw_out'	THEN 'bw_out benefit'
    WHEN LOWER(IMPACT_CATEGORY) = 'disk_stor'	THEN 'disk storage benefit'
    WHEN LOWER(IMPACT_CATEGORY) = 'mssql_stor'	THEN 'mssql storage benefit'
    WHEN LOWER(IMPACT_CATEGORY)  like 'spl_dsc%'	 THEN 'discount - special'		-- new logic_jcm
    WHEN LOWER(B.EVENT_TYPE) LIKE '%_bwout' AND LOWER(C.IMPACT_CATEGORY) like '%bw_out'	THEN CONCAT('bandwidth - tier ' , substr(IMPACT_CATEGORY, 1, 1))
    WHEN LOWER(B.EVENT_TYPE) LIKE '%_bwcdn' AND (LOWER(IMPACT_CATEGORY) like '%bw_cdn%'	OR  LOWER(IMPACT_CATEGORY)  like  '%bw_atari%') THEN CONCAT('discount - tier ' ,  substr(IMPACT_CATEGORY, 1, 1))
  WHEN LOWER(B.EVENT_TYPE) LIKE '%cdn_bwout' AND  (LOWER(IMPACT_CATEGORY)  like '%bw_cdn%'  OR  LOWER(IMPACT_CATEGORY)  like  '%cdbout') THEN CONCAT('discount - tier ' , substr(IMPACT_CATEGORY, 1, 1)   ) 	-- new logic_jcm
    ELSE
	    'discount'  
    END 
    ) 
    WHEN  LOWER(B.EVENT_TYPE) = '/event/billing/cycle/tax'
    THEN   (CASE C.TAX_TYPE_ID
						WHEN 0 THEN 'federal tax'
						WHEN 1 THEN 'state tax'
						WHEN 2 THEN 'county tax'
						WHEN 3 THEN 'local sales tax'
						WHEN 8 THEN 'local sales tax'
						ELSE 'other tax' END	  )
    END						    AS IMPACT_TYPE_DESCRIPTION,
    CASE
    WHEN LOWER(IMPACT_CATEGORY)  like '%managed%'										THEN 1
    WHEN LOWER(B.EVENT_TYPE) IN('/event/delayed/rax/cloud/bigdata_uptime',
    '/event/delayed/rax/cloud/database_compute',	
    '/event/delayed/rax/cloud/monitoring',
    '/event/delayed/rax/cloud/ldbal_uptime',
    '/event/delayed/rax/cloud/site_ssl_cert',
    '/event/delayed/rax/cloud/server_uptime',
    '/event/delayed/rax/cloud/legacy_server_uptime')
    THEN C.EBI_QUANTITY/3600
    WHEN LOWER(B.EVENT_TYPE) IN ('/event/delayed/rax/cloud/bigdata_bwout',
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
    AND LOWER(C.IMPACT_CATEGORY)  not like '%managed%'
    THEN C.EBI_QUANTITY/1024
    WHEN LOWER(B.EVENT_TYPE) IN ('/event/delayed/rax/cloud/cbckup_license') THEN C.EBI_QUANTITY
    WHEN LOWER(B.EVENT_TYPE) IN ('/event/delayed/rax/cloud/database_storage',
    '/event/delayed/rax/cloud/cbs_storage',
    '/event/delayed/rax/cloud/cbs_volume') THEN C.EBI_QUANTITY/61440
    WHEN LOWER(EVENT_TYPE) IN ('/event/delayed/rax/cloud/ldbal_conn') THEN EBI_QUANTITY/100
    WHEN LOWER(EVENT_TYPE) IN ('/event/delayed/rax/cloud/site_met_usage')  and LOWER(C.rum_name)=  'bw_out'  THEN EBI_QUANTITY/1024  
    WHEN LOWER(EVENT_TYPE) IN ('/event/delayed/rax/cloud/site_met_usage')  and LOWER(rum_name) =  'comp_cycle'  THEN EBI_QUANTITY 
    WHEN LOWER(EVENT_TYPE) IN ('/event/delayed/rax/cloud/server_ip', '/event/delayed/rax/cloud/legacy_server_ip')   THEN EBI_QUANTITY/60
    WHEN (LOWER(EVENT_TYPE)  like '/event/delayed/rax/cloud/database%'  OR LOWER(EVENT_TYPE)  like '/event/delayed/rax/cloud/server%'	
    OR LOWER(EVENT_TYPE) like '/event/delayed/rax/cloud/legacy_server%')	
    AND LOWER(IMPACT_CATEGORY) like '%managed%'		
    THEN 1
    ELSE     EBI_QUANTITY						 END 	AS QUANTITY,
    (CASE 
    WHEN ( EBI_IMPACT_TYPE = 258 )
    AND LOWER(EVENT_TYPE) IN ('/event/delayed/rax/cloud/monitoring',
    '/event/delayed/rax/cloud/database_compute',
    '/event/delayed/rax/cloud/server_uptime',
    '/event/delayed/rax/cloud/legacy_server_uptime') THEN 'hours'
    WHEN (EBI_IMPACT_TYPE = 258 ) 
    AND LOWER(EVENT_TYPE) IN (
    '/event/delayed/rax/cloud/server_bwout',
    '/event/delayed/rax/cloud/legacy_server_bwout')  THEN 'gb'		
    WHEN (EBI_IMPACT_TYPE = 258 )
    AND LOWER(EVENT_TYPE) IN ('/event/delayed/rax/cloud/database_storage',
    '/event/delayed/rax/cloud/cbs_volume',
    '/event/delayed/rax/cloud/cbs_storage')         THEN 'gb_hrs'
     WHEN ( EBI_IMPACT_TYPE = 1 )
    AND LOWER(EVENT_TYPE) IN ('/event/billing/product/fee/purchase')
    AND LOWER(SERVICE_OBJ_TYPE) IN ('/service/rax/cloud/site/domain')
										   THEN 'occurrence'
    WHEN LOWER(IMPACT_CATEGORY) like '%managed%' THEN	'occurrence'			 -- new logic_jcm
    WHEN  LOWER(EVENT_TYPE) = '/event/activity/rax/fastlane'  THEN	 'service'	 -- new logic_jcm
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
    WHEN LOWER(IMPACT_CATEGORY)	like '%managed%'  THEN  cast(EBI_AMOUNT as STRING)		-- new logic_jcm
    WHEN LOWER(SERVICE_TYPE) like '/service/rax/cloud/site/domain'	THEN cast(EBI_AMOUNT as STRING)		
    WHEN LOWER(EVENT_TYPE) = '/event/billing/cycle/tax'	THEN NULL
    WHEN LOWER(EVENT_TYPE) = '/event/activity/rax/fastlane' THEN '1'			-- new logic_jcm
    WHEN (EBI_IMPACT_TYPE = 1 AND LOWER(EVENT_TYPE) in ('/event/billing/product/fee/cycle/cycle_forward_annual','/event/billing/product/fee/purchase') )	THEN NULL
    WHEN EBI_RATE_TAG LIKE '%|%'    THEN substring(EBI_RATE_TAG,1, strpos(EBI_RATE_TAG,'|')-1)		
    END		AS  RATE,
    CASE      WHEN LOWER(EVENT_TYPE) = '/event/billing/cycle/tax'				--|| v_TaxEventType ||
			THEN IFNULL( TAX_AMOUNT    ,  EBI_AMOUNT)		-- when tax record is not found, use ebi value_jcm09.22.16
			ELSE EBI_AMOUNT     
	END					  AS AMOUNT,
    C.EBI_CURRENCY_ID,	-- new field added 12.10.15jcm
    EVENT_CREATED_DATE,		-- new field added 2/29/2016kvc
    C.EBI_PRODUCT_OBJ_Type,   -- new field added 5.24.16_kvc												
    C.EBI_GL_ID,			 -- new field added 5.24.16_kvc		   
    current_date() as tblload_dtt,
    ACTIVITY_SERVICE_TYPE				AS EVENT_FASTLANE_SERVICE_TYPE,
    ACTIVITY_EVENT_TYPE					AS EVENT_FASTLANE_EVENT_TYPE,
    ACTIVITY_RECORD_ID					AS EVENT_FASTLANE_RECORD_ID,
    ACTIVITY_DC_ID						AS EVENT_FASTLANE_DC_ID,
    ACTIVITY_REGION					     AS EVENT_FASTLANE_REGION,
    ACTIVITY_RESOURCE_ID				     AS EVENT_FASTLANE_RESOURCE_ID,
    ACTIVITY_RESOURCE_NAME				AS EVENT_FASTLANE_RESOURCE_NAME,
    ACTIVITY_ATTR1						AS EVENT_FASTLANE_ATTR1,
    ACTIVITY_ATTR2						AS EVENT_FASTLANE_ATTR2,
    ACTIVITY_ATTR3						AS EVENT_FASTLANE_ATTR3,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
    EVENT_TYPE						     AS EVENT_POID_TYPE,
    EVENT_EARNED_START_DATE,			-- new field added 5.2.19_kvc 			
    EVENT_EARNED_END_DATE,			-- new field added 5.2.19_kvc 	
    IFNULL(ACTIVITY_IS_BACKBILL,0)		     AS EVENT_FASTLANE_IS_BACKBILL,-- new field added 5.2.19_kvc 		
    EBI_OFFERING_OBJ_ID0,			-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_DEAL_CODE,		-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_GRP_CODE,			-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_SUB_GRP_CODE,		-- new field added 5.2.19_kvc 	
    IFNULL(FASTLANE_IMPACT_IS_BACKBILL,0)   AS FASTLANE_IMPACT_IS_BACKBILL,-- new field added 5.2.19_kvc
    0							    AS Is_Backbill ,
	B.ACTIVITY_ATTR4 -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,B.ACTIVITY_ATTR5 -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,B.ACTIVITY_ATTR6	-- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,Service_Obj_Id0 -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,C.Bill_Created_Date  --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging

FROM
  stage_one.raw_ssis_invoice_load_step1 A
INNER JOIN
   stage_one.raw_ssis_event_load_step2 B
ON A.ITEM_POID_ID0=B.EVENT_Item_Obj_Id0
INNER JOIN
  stage_one.raw_ssis_impacts_load_step3  C
ON  B.EVENT_POID_ID0=C.IMPACTBAL_EVENT_OBJ_ID0
);   
-------------------------------------------------------------------------------------------------------------------

UPDATE
    stage_one.raw_invitemeventdetail_daily_stage
SET
    Is_Backbill=1     
where
    (CAST(EVENT_EARNED_END_DATE as date) <> '1970-01-01')
AND IFNULL(EVENT_EARNED_END_DATE,'1970-01-01') <= IFNULL(BILL_START_DATE,'1970-01-01')
AND IFNULL(Is_Backbill,0)=0;
------------------------------------------------------------------------------------------------------------
UPDATE
    stage_one.raw_invitemeventdetail_daily_stage
SET
    Is_Backbill=1
    
where
   IFNULL(FASTLANE_IMPACT_IS_BACKBILL,0)<>0
AND IFNULL(Is_Backbill,0)=0;
------------------------------------------------------------------------------------------------------------
UPDATE
    stage_one.raw_invitemeventdetail_daily_stage
SET
    Is_Backbill=1
    
where
   IFNULL(EVENT_FASTLANE_IS_BACKBILL,0)<>0
AND IFNULL(Is_Backbill,0)=0;
------------------------------------------------------------------------------------------------------------
UPDATE
    stage_one.raw_invitemeventdetail_daily_stage
SET
    Is_Backbill=1
     
where
   IFNULL(LOWER(FASTLANE_IMPACT_SUB_GRP_CODE),'unknown') like  '%backbill%'
AND IFNULL(Is_Backbill,0)=0;
------------------------------------------------------------------------------------------------------------
UPDATE
    stage_one.raw_invitemeventdetail_daily_stage
SET
    PROD_DECSRIPTION='tax',
    PRODUCT_NAME=IMPACT_TYPE_DESCRIPTION,
    PRODUCT_CODE=IMPACT_TYPE_DESCRIPTION

WHERE 
    LOWER(EVENT_TYPE)  LIKE '%tax%' ;
-------------------------------------------------------------------------------------------------------------------
UPDATE
    stage_one.raw_invitemeventdetail_daily_stage A
SET
    A.EBI_PRODUCT_OBJ_ID0=B.Product_ID,
    A.PRODUCT_POID_ID0=B.Product_ID
FROM
    stage_two_dw.stage_cloud_hosting_products B 

WHERE
A.Product_Name = B.Product_Name AND
    LOWER(EVENT_TYPE)  LIKE '%tax%' 
AND LOWER(Product_ID_NK) like '%brm_%';
-------------------------------------------------------------------------------------------------------------------
UPDATE 
 stage_one.raw_invitemeventdetail_daily_stage
SET
    EVENT_EARNED_START_DATE = '1900-01-01' 
 
WHERE  
    EVENT_EARNED_START_DATE IS NULL;
-------------------------------------------------------------------------------------------------------------------
UPDATE 
stage_one.raw_invitemeventdetail_daily_stage
SET
    EVENT_EARNED_START_DATE = '1900-01-01' 

WHERE  
    EVENT_EARNED_START_DATE= '1970-01-01';
-------------------------------------------------------------------------------------------------------------------
UPDATE 
 stage_one.raw_invitemeventdetail_daily_stage
SET
    EVENT_EARNED_END_DATE = '1900-01-01' 
 
 WHERE  
    EVENT_EARNED_END_DATE IS NULL;
-------------------------------------------------------------------------------------------------------------------
UPDATE 
 stage_one.raw_invitemeventdetail_daily_stage
SET
    EVENT_EARNED_END_DATE = '1900-01-01' 
 
WHERE  
    EVENT_EARNED_END_DATE= '1970-01-01';
-------------------------------------------------------------------------------------------
END;
