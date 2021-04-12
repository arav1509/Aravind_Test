CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_load_stage_invitemeventdetail_incremental`()
BEGIN
----------------------------------------------- 
DELETE FROM     
 stage_two_dw.stage_invitemeventdetail  
WHERE    
   EXISTS    
(    
SELECT    
 DETAIL_UNIQUE_RECORD_ID    
FROM    
 (SELECT DISTINCT     -- INTO  #Modified_Tickets
    DETAIL_UNIQUE_RECORD_ID    
   
FROM      
    stage_one.raw_invitemeventdetail_daily_stage A        
WHERE  

  CAST(BILL_END_DATE as DATETIME)  > '2014-12-31 00:00:00.000' ) XX    
WHERE    
 XX.DETAIL_UNIQUE_RECORD_ID =stage_invitemeventdetail.DETAIL_UNIQUE_RECORD_ID    
) ;   
-----------------------------------------------------------------------------------------------------------   
INSERT INTO    
   stage_two_dw.stage_invitemeventdetail     
SELECT     
    DETAIL_UNIQUE_RECORD_ID,    
    ACCOUNT_POID_ID0,    
    BRM_ACCOUNTNO,    
    ACCOUNT_ID,    
    GL_SEGMENT,    
    BILL_POID_ID0,    
    A.BILL_NO,    
    BILL_START_DATE ,    
    BILL_END_DATE ,    
    BILL_MOD_DATE ,    
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
    EVENT_TYPE,    
    RERATE_OBJ_ID0,    
    BATCH_ID,    
    EVENT_NAME,    
    EVENT_SYS_DESCR,    
    EVENT_RUM_NAME,    
    PRODUCT_POID_ID0,    
    PROD_DECSRIPTION,    
    PRODUCT_NAME,    
    PRODUCT_CODE,    
    IMPACTBAL_EVENT_OBJ_ID0,    
    IMPACT_CATEGORY,    
    EBI_IMPACT_TYPE,    
    EBI_AMOUNT,    
    EBI_QUANTITY,    
    EBI_RATE_TAG,    
    EBI_REC_ID,    
    EBI_RUM_ID,    
    EBI_PRODUCT_OBJ_ID0,    
    USAGE_RECORD_ID,    
    DC_ID,    
    REGION_ID,    
    RES_ID,    
    RES_NAME,    
    MANAGED_FLAG,    
    TAX_REC_ID,    
    TAX_NAME,    
    TAX_TYPE_ID,    
    TAX_ELEMENT_ID,    
    TAX_AMOUNT,    
    TAX_RATE_PERCENT,    
    INVOICE_SERVICEITEM_DESCR,    
    PROD_CATEGORY,    
    IMPACT_TYPE_DESCRIPTION,    
    Invoice_serviceItem_Descr,    
    EVENT_Category,    
    QUANTITY,    
    UOM,    
    RATE,    
    AMOUNT,    
    tblload_dtt,    
    EBI_CURRENCY_ID,    -- new field added 12.10.15jcm    
    EVENT_CREATED_DATE,    -- new field added 2/29/2016kvc       
    EBI_GL_ID,      -- new field added 5.24.16_kvc      
    EBI_PRODUCT_OBJ_Type,   -- new field added 5.24.16_kvc    
    EVENT_FASTLANE_SERVICE_TYPE, -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_EVENT_TYPE,  -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_RECORD_ID,  -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_DC_ID,   -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_REGION,   -- new field added 1.20.17_kvc        
    EVENT_FASTLANE_RESOURCE_ID,  -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_RESOURCE_NAME, -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_ATTR1,   -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_ATTR2,   -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_ATTR3,   -- new field added 1.20.17_kvc    
    FASTLANE_IMPACT_CATEGORY,  -- new field added 1.20.17_kvc     
    FASTLANE_IMPACT_VALUE,   -- new field added 1.20.19_kvc     
    EVENT_POID_TYPE,    -- new field added 1.30.19_kvc    
    EVENT_EARNED_START_DATE,   -- new field added 5.2.19_kvc        
    EVENT_EARNED_END_DATE,   -- new field added 5.2.19_kvc      
    EVENT_FASTLANE_IS_BACKBILL,  -- new field added 5.2.19_kvc      
    CAST(EBI_OFFERING_OBJ_ID0 as INT64) AS EBI_OFFERING_OBJ_ID0,   -- new field added 5.2.19_kvc      
    FASTLANE_IMPACT_DEAL_CODE,  -- new field added 5.2.19_kvc      
    FASTLANE_IMPACT_GRP_CODE,   -- new field added 5.2.19_kvc      
    FASTLANE_IMPACT_SUB_GRP_CODE,  -- new field added 5.2.19_kvc      
    FASTLANE_IMPACT_IS_BACKBILL,  -- new field added 5.2.19_kvc      
    Is_Backbill     -- new field added 5.14.19_kvc    
 ,TO_BASE64((MD5(CONCAT( BILL_NO,'|',
                                                 ACCOUNT_POID_ID0,'|',
												 UOM,'|',
												 IMPACT_CATEGORY ,'|',
												 EBI_IMPACT_TYPE,'|',
												 Impact_Type_Description,
												 Service_Obj_Type,'|'          
												 ) )))  Invoice_Nk,
 'Bill_Number|Billing_Application_Account_Number|Unit_Measure_Of_Code|Impact_Category|Impact_Type_Id|Impact_Type_Description|Service_Type' Invoice_Source_field_NK     
 , TO_BASE64((MD5(
CONCAT(CAST(COALESCE(EVENT_FASTLANE_SERVICE_TYPE,FASTLANE_IMPACT_GRP_CODE,'N/A') AS STRING) ,'|'
	, CAST(COALESCE(EVENT_FASTLANE_EVENT_TYPE,FASTLANE_IMPACT_SUB_GRP_CODE,'N/A')AS STRING ),'|'
	, CAST(COALESCE(EVENT_FASTLANE_DC_ID,DC_ID,'N/A') AS  STRING),'|'
	, CAST(COALESCE(EVENT_FASTLANE_REGION,'N/A') AS STRING),'|'
	, CAST(COALESCE(EVENT_FASTLANE_RESOURCE_ID,Res_Id,'N/A')  AS STRING),'|'
	, CAST(COALESCE(EVENT_FASTLANE_RESOURCE_NAME,RES_NAME,'N/A')  AS STRING)    ,'|'
	, CAST(COALESCE(EVENT_FASTLANE_ATTR1,'N/A')  AS STRING )  ,'|'
	, CAST(COALESCE(EVENT_FASTLANE_ATTR2,'N/A') AS STRING )    ,'|'
	, CAST(COALESCE(EVENT_FASTLANE_ATTR3,'N/A') AS STRING)      ,'|'
	, CAST(COALESCE(ACTIVITY_ATTR4,'N/A') AS STRING)    ,'|'
	, CAST(COALESCE(ACTIVITY_ATTR5,'N/A') AS STRING)      ,'|'
	, CAST(COALESCE(ACTIVITY_ATTR6,'N/A') AS STRING)    ,'|'
	, CAST('N/A' AS STRING)    ,'|'
	,CAST('N/A' AS STRING)    ,'|'
	, CAST(COALESCE(FASTLANE_IMPACT_CATEGORY,'N/A') AS STRING)     ,'|'
	, CAST(COALESCE(FASTLANE_IMPACT_VALUE,'N/A') AS STRING)      ,'|'
	, CAST(COALESCE(FASTLANE_IMPACT_DEAL_CODE,'N/A') AS STRING)     ,'|'
	--, COALESCE(Usage_Record_Id,EVENT_FASTLANE_RECORD_ID,'N/A') ,'|'
	,CAST( COALESCE(RATE,'N/A') AS STRING) ,'|'
	, CAST(COALESCE(EBI_rate_tag,'N/A') AS STRING),'|'
	,CAST(COALESCE(Tax_Type_Id ,0) AS STRING) ,'|'
	,CAST(COALESCE(TAX_ELEMENT_ID,0) AS STRING),'|'					  
	) ))) AS Invoice_Attribute_NK    
 ,'Invoice_Group_Code|Invoice_Sub_Group_Code|Fastlane_Data_Center_Id|Fastlane_Region|Invoice_Resource_Id|Invoice_Resource_Name|Fastlane_Attr1|Fastlane_Attr2|Fastlane_Attr3|Fastlane_Attr4|Fastlane_Attr5|Fastlane_Attr6|Fastlane_Attr7|Fastlane_Attr8|Fastlane_Impact_Category|Fastlane_Impact_Value|Deal_Code|Rate|Rate_Tag|Tax_Type_Id|Tax_Element_Id'
	,ACTIVITY_ATTR4  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,ACTIVITY_ATTR5  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,ACTIVITY_ATTR6  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,Service_Obj_Id0 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
    ,'N/A' as  ACTIVITY_ATTR7 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging No mapping just hardcoded column
	,'N/A' as  ACTIVITY_ATTR8 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging No mapping just hardcoded column
	,
     CAST(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"),INTERVAL cast(Bill_Created_Date  as int64)second) As datetime) As bill_created_date,                                    --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
 ---CAST(date_add(ss,Bill_Created_Date,'1970-01-01') as datetime)  As bill_created_date
 
  CAST(0 as INT64) AS is_transaction_successful,
  'Cloud' AS Global_Account_Type
	,
  CASE 
WHEN A.Item_Type = '/item/adjustment' THEN 'adjustment'
WHEN A.Item_Type = '/item/payment' THEN 'payment'
WHEN A.Item_Type = '/item/payment/reversal' THEN 'reversal'
WHEN A.Item_Type = '/item/purchase' THEN 'purchase'
WHEN A.Item_Type = '/item/cycle_tax' THEN 'tax'
ELSE NULL END AS Item_Tag
  FROM    
    
(SELECT     --INTO  #InvItemEventDetail_Daily_Stage 
 A.DETAIL_UNIQUE_RECORD_ID,  
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
    USAGE_RECORD_ID,  
    DC_ID,  
    REGION_ID,  
    RES_ID,  
    RES_NAME,  
    --------------------------------------  
    MANAGED_FLAG,  
    TAX_REC_ID,  
    TAX_NAME,  
    TAX_TYPE_ID,  
    TAX_ELEMENT_ID,  
    TAX_AMOUNT,  
    TAX_RATE_PERCENT,  
    INVOICE_SERVICEITEM_DESCR,  
    PROD_CATEGORY,  
    IMPACT_TYPE_DESCRIPTION,  
   -- (IMPACT_TYPE_DESCRIPTION + '--' + IFNULL(EBI_RATE_TAG , IFNULL(PROD_CATEGORY,PROD_DECSRIPTION)) +'--'+ITEM_NAME) as Invoice_Item_Desc,    
    CASE     
    WHEN     
    lower(IMPACT_TYPE_DESCRIPTION) LIKE 'discount%'     
    THEN      
    IMPACT_TYPE_DESCRIPTION    
    ELSE     
    (CASE     
    WHEN lower(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/glance'            THEN 'glance'           
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/site_met_usage'            THEN'sites metered usage'     
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/database_compute' THEN    
     (CASE WHEN   lower(impact_category)  = 'managed'    THEN 'manage service level'     
      else  'database compute' END)    
  WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/database_storage'           THEN 'database storage'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/monitoring'           THEN 'monitoring'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/cbs_storage'          THEN 'cbs storage'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/cbs_volume'              THEN  'cbs volume'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/cbckup_license'      THEN  'backup license'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/legacy_server_uptime' THEN    
     (CASE    
     --WHEN PROD_CATEGORY2 IS NOT NULL  AND  PROD_CATEGORY2<> 0 AND  PROD_CATEGORY2 =  'SUPPORT' THEN      
     WHEN PROD_CATEGORY IS NOT NULL  AND  PROD_CATEGORY <> '0' AND  lower(PROD_CATEGORY) =  'support' THEN  --  'Legacy Uptime Support - Managed xxx'    
      (CASE    
         WHEN  managed_flag  IS NOT NULL   AND  managed_flag  = 2   THEN   'legacy uptime support - managed infrastructure'          
        WHEN  (managed_flag  IS NOT NULL   AND  managed_flag  = 3)  THEN   'legacy uptime support - managed operations sysops'      
        WHEN  (managed_flag  IS NOT NULL  AND  managed_flag  = 4)  THEN   'legacy uptime support - managed operations devops automation'        
        else   'manage service level' END)        
      else  'legacy server uptime'    END)    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/legacy_server_bwout'          THEN  'legacy server bwout'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/legacy_server_ip'                 THEN  'legacy server ip'    
    WHEN lower(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/server_uptime'   THEN     
     (CASE     
      WHEN  (PROD_CATEGORY IS NOT NULL AND  PROD_CATEGORY <> '0' AND  lower(PROD_CATEGORY) =  'support') THEN  --  'ng uptime support - managed xxx'    
      (CASE     
      WHEN ( managed_flag  IS NOT NULL  AND  managed_flag  = 2)   THEN  'ng uptime support - managed infrastructure'         
      WHEN (managed_flag  IS NOT NULL  AND  managed_flag  = 3)  THEN  'ng uptime support - managed operations sysops'        
      WHEN (managed_flag  IS NOT NULL  AND  managed_flag  = 4) THEN   'ng uptime support - managed operations devops automation'         
      WHEN  (  impact_category IS NOT NULL  AND impact_category  <> '0'  AND    lower(impact_category) =  'managed')     
         THEN   'manage service level' END)    
     ELSE  'ng server uptime' END)            
               
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/server_bwout'                THEN  'ng server bwout'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/server_ip'                 THEN  'ng server ip'    
    WHEN lower(A.EVENT_TYPE)   IN ('/event/delayed/rax/cloud/files_bwout','/event/delayed/rax/cloud/cdn_bwout')             THEN  'files bwout'    
    WHEN lower(A.EVENT_TYPE)   In ('/event/delayed/rax/cloud/files_bwcdn','/event/delayed/rax/cloud/cdn_requests')      THEN  'files cdn bw'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/bigdata_uptime'        THEN  'big data uptime'    
    WHEN lower(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/queue_api'                 THEN  'cloud queue api'    
    WHEN lower(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/queue_bwout'             THEN  'cloud queue bwout'    
    WHEN lower(A.EVENT_TYPE) = '/event/billing/cycle/tax'       THEN  'tax'    
    
    WHEN (lower(A.EVENT_TYPE) =  '/event/billing/cycle/discount' OR  lower(A.EVENT_TYPE) =  '/event/billing/cycle/fold')  THEN     
     ( CASE WHEN lower(EBI_RATE_TAG) =  'support fee cycle fold'  THEN     
      ( CASE  WHEN  (impact_category IS NOT NULL   AND lower(impact_category) IN ('ops_minchg_sys_infra','sysops_infra'))    
          THEN   'minimum support fee - managed infrastructure'    
         WHEN  (  impact_category IS NOT NULL   AND   lower(impact_category) IN ('ops_minchg_sys_managed','sysops_managed'))    
          THEN   'minimum support fee - managed operations sysops'       
        ---EBI_RATE_TAG = EBI_RATE_TAG_arr     
      END)     
      ELSE   'billing time charges' END )          
    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/bigdata_bwout'          THEN  'bigdata bwout'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/ldbal_bwout'                THEN  'loadbalancer bwout'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/ldbal_uptime'            THEN  'loadbalancer uptime'    
    WHEN lower(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/site_ssl_cert'              THEN  'sites ssl cert'    
    WHEN lower(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/cdn_ssl_cert'      THEN 'files ssl cert'    
    WHEN lower(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/files_stor'             THEN  'files store'    
    WHEN lower(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/site_stor'                  THEN  'sites store'    
    WHEN lower(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/ldbal_conn'              THEN  'loadbalancer conn'    
    WHEN lower(SERVICE_OBJ_TYPE) =  '/service/rax/fastlane/aws'        THEN 'aws'    
    WHEN lower(SERVICE_OBJ_TYPE) =  '/service/rax/fastlane/azure'        THEN 'azure'    
    WHEN lower(SERVICE_OBJ_TYPE) =   '/service/rax/fastlane/cas'       THEN 'critical app services'    
    WHEN lower(SERVICE_OBJ_TYPE) =   '/service/rax/fastlane/dba_services'      THEN 'dba services'    
    WHEN lower(SERVICE_OBJ_TYPE) = '/service/rax/fastlane/digital'       THEN 'digital'    
     WHEN lower(SERVICE_OBJ_TYPE) = '/service/rax/fastlane/rms'        THEN 'rackspace managed security'    
    WHEN (lower(A.EVENT_TYPE) =  '/event/billing/product/fee/cycle/cycle_forward_monthly'     
      OR  lower(A.EVENT_TYPE) = '/event/billing/product/fee/cycle/cycle_forward_annual'     
      OR  lower(A.EVENT_TYPE) =   '/event/billing/product/fee/purchase')    
     THEN  'Other Charges - Product Fees' --'Other Charges'    
    WHEN lower(A.EVENT_TYPE) = '/event/delayed/rax/cloud/server_license' THEN 'server license fee'    
  ELSE    A.EVENT_TYPE END )    
  END   as EVENT_Category,    
    QUANTITY,    
    UOM,    
    RATE,    
    AMOUNT,    
    tblload_dtt,    
    EBI_CURRENCY_ID,    -- new field added 12.10.15jcm    
    EVENT_CREATED_DATE,    -- new field added 2/29/2016kvc    
    EBI_GL_ID,      -- new field added 5.24.16_kvc     
    EBI_PRODUCT_OBJ_Type,   -- new field added 5.24.16_kvc     
    EVENT_FASTLANE_SERVICE_TYPE, -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_EVENT_TYPE,  -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_RECORD_ID,  -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_DC_ID,   -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_REGION,   -- new field added 1.20.17_kvc        
    EVENT_FASTLANE_RESOURCE_ID,  -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_RESOURCE_NAME, -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_ATTR1,   -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_ATTR2,   -- new field added 1.20.17_kvc    
    EVENT_FASTLANE_ATTR3,   -- new field added 1.20.17_kvc    
    FASTLANE_IMPACT_CATEGORY,  -- new field added 1.20.17_kvc     
    FASTLANE_IMPACT_VALUE,   -- new field added 1.20.19_kvc     
    EVENT_POID_TYPE,    -- new field added 1.30.19_kvc    
    EVENT_EARNED_START_DATE,   -- new field added 5.2.19_kvc        
    EVENT_EARNED_END_DATE,   -- new field added 5.2.19_kvc      
    EVENT_FASTLANE_IS_BACKBILL,  -- new field added 5.2.19_kvc      
    CAST(EBI_OFFERING_OBJ_ID0 AS INT64) AS EBI_OFFERING_OBJ_ID0,   -- new field added 5.2.19_kvc      
    FASTLANE_IMPACT_DEAL_CODE,  -- new field added 5.2.19_kvc      
    FASTLANE_IMPACT_GRP_CODE,   -- new field added 5.2.19_kvc      
    FASTLANE_IMPACT_SUB_GRP_CODE,  -- new field added 5.2.19_kvc      
    FASTLANE_IMPACT_IS_BACKBILL,  -- new field added 5.2.19_kvc    
    Is_Backbill     -- new field added 5.14.19_kvc
	,ACTIVITY_ATTR4  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,ACTIVITY_ATTR5  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,ACTIVITY_ATTR6  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,Service_Obj_Id0 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,Bill_Created_Date --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
  
FROM    
    stage_one.raw_invitemeventdetail_daily_stage A     
INNER JOIN    
    ( SELECT    --INTO  #NEW_Invoices 
    DETAIL_UNIQUE_RECORD_ID    
   
FROM    
    stage_one.raw_invitemeventdetail_daily_stage A       
WHERE    
  CAST(BILL_END_DATE as DATETIME)  > '2014-12-31 00:00:00.000'    
AND NOT EXISTS    
(    
SELECT    
 DETAIL_UNIQUE_RECORD_ID    
FROM    
 stage_two_dw.stage_invitemeventdetail XX       
WHERE    
 XX.DETAIL_UNIQUE_RECORD_ID =A.DETAIL_UNIQUE_RECORD_ID    
)    ) B    
ON A.DETAIL_UNIQUE_RECORD_ID =B.DETAIL_UNIQUE_RECORD_ID  ) A   ; 
    
END;
