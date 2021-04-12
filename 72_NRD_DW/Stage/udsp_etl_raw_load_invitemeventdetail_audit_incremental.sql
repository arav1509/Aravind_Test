CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_load_invitemeventdetail_audit_incremental`()
BEGIN

---------------------------------------------------------------------------------------
/*
Created On: 10/30/2014
Created By: Kano Cannick

Description: Script to load new and modified records into Stage_InvItemEventDetail.
Modifications:
Modified By    Date			  Description
jcm			 12.10.15		  added currency fields
kcannick     5.24.16		  added EBI_GL_ID,EBI_PRODUCT_OBJ_Type
RAHU4260     11.27.19         migrated from ABO to EBI-ETL Server 
*/



CREATE OR REPLACE TEMP TABLE Raw_InvItemEventDetail_Daily_Audit_Stage AS
SELECT --INTO    #Raw_InvItemEventDetail_Daily_Audit_Stage
    A.DETAIL_UNIQUE_RECORD_ID,
    ACCOUNT_POID_ID0,
    BRM_ACCOUNTNO,
    A.ACCOUNT_ID,
    GL_SEGMENT,
    ----------------------------------------
    BILL_POID_ID0,
    A.BILL_NO,
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
    concat(IMPACT_TYPE_DESCRIPTION , '--' , ifnull(EBI_RATE_TAG , ifnull(PROD_CATEGORY,PROD_DECSRIPTION)) ,'--',ITEM_NAME) as Invoice_Item_Desc,
    CASE 
    WHEN 
    IMPACT_TYPE_DESCRIPTION LIKE 'DISCOUNT%' 
    THEN  
    IMPACT_TYPE_DESCRIPTION
    ELSE 
    (CASE 
		  WHEN LOWER(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/glance'           	THEN 'Glance'       
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/site_met_usage'           	THEN'Sites Metered Usage' 
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/database_compute' THEN
			  (CASE WHEN   UPPER(impact_category)  = 'MANAGED'  		THEN 'Manage Service Level' 
				  else 	'Database Compute' END)
	 WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/database_storage'          	THEN 'Database Storage'
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/monitoring'          	THEN 'Monitoring'
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/cbs_storage'         	THEN 'CBS Storage'
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/cbs_volume'             	THEN  'CBS Volume'
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/cbckup_license'	     THEN  'Backup License'
		   WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/legacy_server_uptime'	THEN
			  (CASE
			  --WHEN PROD_CATEGORY2 IS NOT NULL  AND  PROD_CATEGORY2<> 0 AND  PROD_CATEGORY2 =  'SUPPORT' THEN  
			  WHEN PROD_CATEGORY IS NOT NULL  AND  PROD_CATEGORY <> '0' AND  PROD_CATEGORY =  'SUPPORT' THEN		--  'Legacy Uptime Support - Managed xxx'
				  (CASE
						   WHEN  managed_flag  IS NOT NULL   AND  managed_flag  = 2 		THEN   'Legacy Uptime Support - Managed Infrastructure'						
						  WHEN  (managed_flag  IS NOT NULL   AND  managed_flag  = 3) 	THEN   'Legacy Uptime Support - Managed Operations SysOps'		
						  WHEN  (managed_flag  IS NOT NULL  AND  managed_flag  = 4) 	THEN   'Legacy Uptime Support - Managed Operations DevOps Automation' 			
						  else   'Manage Service Level' END) 			
				  else  'Legacy Server Uptime' 			END)
		  WHEN LOWER(A.EVENT_TYPE)=   '/event/delayed/rax/cloud/legacy_server_bwout'		       	THEN  'Legacy Server BWOUT'
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/legacy_server_ip'		               	THEN  'Legacy Server IP'
		  WHEN LOWER(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/server_uptime'			THEN 
			  (CASE 
				  WHEN  (PROD_CATEGORY IS NOT NULL AND  PROD_CATEGORY <> '0' AND  PROD_CATEGORY =  'SUPPORT') THEN		--  'NG Uptime Support - Managed xxx'
				  (CASE 
				  WHEN ( managed_flag  IS NOT NULL  AND  managed_flag  = 2)   THEN  'NG Uptime Support - Managed Infrastructure'					
				  WHEN (managed_flag  IS NOT NULL  AND  managed_flag  = 3)  THEN  'NG Uptime Support - Managed Operations SysOps'				
				  WHEN (managed_flag  IS NOT NULL  AND  managed_flag  = 4) THEN   'NG Uptime Support - Managed Operations DevOps Automation'					
				  WHEN  (  impact_category IS NOT NULL  AND impact_category  <> '0'  AND    impact_category =  'MANAGED') 
							  THEN   'Manage Service Level' END)
			  ELSE  'NG Server Uptime' END) 							
         		
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/server_bwout'               	THEN  'NG Server BWOUT'
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/server_ip'                	THEN  'NG Server IP'
		  WHEN LOWER(A.EVENT_TYPE)   IN ('/event/delayed/rax/cloud/files_bwout','/event/delayed/rax/cloud/cdn_bwout')            	THEN  'Files BWOUT'
		  WHEN LOWER(A.EVENT_TYPE)   In ('/event/delayed/rax/cloud/files_bwcdn','/event/delayed/rax/cloud/cdn_requests')	     THEN  'Files CDN BW'
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/bigdata_uptime'	      	THEN  'Big Data Uptime'
		  WHEN LOWER(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/queue_api'	              	THEN  'Cloud Queue API'
		  WHEN LOWER(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/queue_bwout'	           	THEN  'Cloud Queue BWOUT'
		  WHEN LOWER(A.EVENT_TYPE) = '/event/billing/cycle/tax'										  	THEN  'TAX'

		  WHEN (LOWER(A.EVENT_TYPE) =  '/event/billing/cycle/discount' OR  A.EVENT_TYPE =  '/event/billing/cycle/fold')  THEN 
			  ( CASE WHEN UPPER(EBI_RATE_TAG) =  'SUPPORT FEE CYCLE FOLD'  THEN 
				  ( CASE  WHEN  (impact_category IS NOT NULL   AND UPPER(impact_category) IN ('OPS_MINCHG_SYS_INFRA','SYSOPS_INFRA'))
								  THEN   'Minimum Support Fee - Managed Infrastructure'
							  WHEN  (  impact_category IS NOT NULL   AND   UPPER(impact_category) IN ('OPS_MINCHG_SYS_MANAGED','SYSOPS_MANAGED'))
								  THEN   'Minimum Support Fee - Managed Operations SysOps'			
						  ---EBI_RATE_TAG = EBI_RATE_TAG_arr 
				  END) 
				  ELSE   'Billing Time Charges' END ) 					

		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/bigdata_bwout'		       	THEN  'Bigdata BWOUT'
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/ldbal_bwout'	              	THEN  'LoadBalancer BWOUT'
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/ldbal_uptime'	          	THEN  'LoadBalancer Uptime'
		  WHEN LOWER(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/site_ssl_cert'	             THEN  'Sites SSL Cert'
		  WHEN LOWER(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/cdn_ssl_cert'			   THEN 'Files SSL Cert'
		  WHEN LOWER(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/files_stor'	            THEN  'Files Store'
		  WHEN LOWER(A.EVENT_TYPE) =   '/event/delayed/rax/cloud/site_stor'	                 THEN  'Sites Store'
		  WHEN LOWER(A.EVENT_TYPE) =  '/event/delayed/rax/cloud/ldbal_conn'		           	THEN  'LoadBalancer CONN'
		  WHEN LOWER(SERVICE_OBJ_TYPE) =  '/service/rax/fastlane/aws'							 THEN 'AWS'
		  WHEN LOWER(SERVICE_OBJ_TYPE) =  '/service/rax/fastlane/azure'							 THEN 'Azure'
		  WHEN LOWER(SERVICE_OBJ_TYPE) =   '/service/rax/fastlane/cas'						 THEN 'Critical App Services'
		  WHEN LOWER(SERVICE_OBJ_TYPE) =   '/service/rax/fastlane/dba_services'					 THEN 'DBA Services'
		  WHEN LOWER(SERVICE_OBJ_TYPE) = '/service/rax/fastlane/digital'						 THEN 'Digital'
		   WHEN LOWER(SERVICE_OBJ_TYPE) = '/service/rax/fastlane/rms'							 THEN 'Rackspace Managed Security'
		  WHEN (LOWER(A.EVENT_TYPE) =  '/event/billing/product/fee/cycle/cycle_forward_monthly' 
				  OR  LOWER(A.EVENT_TYPE) = '/event/billing/product/fee/cycle/cycle_forward_annual' 
				  OR  LOWER(A.EVENT_TYPE) =   '/event/billing/product/fee/purchase')
		   THEN  'Other Charges - Product Fees' --'Other Charges'
    WHEN LOWER(A.EVENT_TYPE) = '/event/delayed/rax/cloud/server_license' THEN 'Server License Fee'
  ELSE 		 A.EVENT_TYPE END )
  END   as EVENT_Category,
    QUANTITY,
    UOM,
    RATE,
    AMOUNT,
    tblload_dtt,
    EBI_CURRENCY_ID,				-- new field added 12.10.15jcm
    EVENT_CREATED_DATE,				-- new field added 2/29/2016kvc			
    EBI_GL_ID,						-- new field added 5.24.16_kvc		
    EBI_PRODUCT_OBJ_Type,			-- new field added 5.24.16_kvc
    EVENT_FASTLANE_SERVICE_TYPE,	-- new field added 1.20.17_kvc
    EVENT_FASTLANE_EVENT_TYPE,		-- new field added 1.20.17_kvc
    EVENT_FASTLANE_RECORD_ID,		-- new field added 1.20.17_kvc
    EVENT_FASTLANE_DC_ID,			-- new field added 1.20.17_kvc
    EVENT_FASTLANE_REGION,			-- new field added 1.20.17_kvc				
    EVENT_FASTLANE_RESOURCE_ID,		-- new field added 1.20.17_kvc
    EVENT_FASTLANE_RESOURCE_NAME,	-- new field added 1.20.17_kvc
    EVENT_FASTLANE_ATTR1,			-- new field added 1.20.17_kvc
    EVENT_FASTLANE_ATTR2,			-- new field added 1.20.17_kvc
    EVENT_FASTLANE_ATTR3,			-- new field added 1.20.17_kvc
    FASTLANE_IMPACT_CATEGORY,		-- new field added 1.20.17_kvc	
    FASTLANE_IMPACT_VALUE,			-- new field added 1.20.17_kvc 
    EVENT_POID_TYPE,
    EVENT_EARNED_START_DATE,			-- new field added 5.2.19_kvc 			
    EVENT_EARNED_END_DATE,			-- new field added 5.2.19_kvc 	
    EVENT_FASTLANE_IS_BACKBILL,			-- new field added 5.2.19_kvc 	
    EBI_OFFERING_OBJ_ID0,			-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_DEAL_CODE,		-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_GRP_CODE,			-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_SUB_GRP_CODE,		-- new field added 5.2.19_kvc 	
    FASTLANE_IMPACT_IS_BACKBILL,		-- new field added 5.2.19_kvc  
    Is_Backbill
	,ACTIVITY_ATTR4  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,ACTIVITY_ATTR5  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,ACTIVITY_ATTR6  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,Service_Obj_Id0 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	,Bill_Created_Date --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
FROM stage_one.raw_invitemeventdetail_daily_audit_stage A;
-----------------------------------------------------------------------------------------------------------
DELETE FROM 
	stage_two_dw.stage_invitemeventdetail
WHERE
   EXISTS
(
SELECT
	DETAIL_UNIQUE_RECORD_ID
FROM
	(
	SELECT DISTINCT --INTO    #Modified_Audit_Tickets
    BILL_NO,
    DETAIL_UNIQUE_RECORD_ID
	FROM stage_one.raw_invitemeventdetail_daily_audit_stage A    
	WHERE  BILL_END_DATE  > date('2014-12-31 00:00:00.000')
	) XX --#Modified_Audit_Tickets XX
WHERE
	XX.BILL_NO =Stage_InvItemEventDetail.BILL_NO
);
-----------------------------------------------------------------------------------------------------------
DELETE FROM 	stage_two_dw.stage_invitemeventdetail
WHERE
   EXISTS
(
SELECT
	DETAIL_UNIQUE_RECORD_ID
FROM
	(
		SELECT DISTINCT --INTO    #Modified_Audit_Tickets
		BILL_NO,
		DETAIL_UNIQUE_RECORD_ID
		FROM stage_one.raw_invitemeventdetail_daily_audit_stage A    
		WHERE  BILL_END_DATE  > date('2014-12-31 00:00:00.000')
	) XX--#Modified_Audit_Tickets XX
WHERE
	XX.DETAIL_UNIQUE_RECORD_ID =Stage_InvItemEventDetail.DETAIL_UNIQUE_RECORD_ID
);
-----------------------------------------------------------------------------------------------------------
INSERT INTO    stage_two_dw.stage_invitemeventdetail(DETAIL_UNIQUE_RECORD_ID,ACCOUNT_POID_ID0,BRM_ACCOUNTNO,ACCOUNT_ID,GL_SEGMENT,BILL_POID_ID0,BILL_NO,BILL_START_DATE,BILL_END_DATE,BILL_MOD_DATE,ITEM_POID_ID0,ITEM_EFFECTIVE_DATE,ITEM_MOD_DATE,ITEM_NAME,ITEM_STATUS,SERVICE_OBJ_TYPE,ITEM_TYPE,EVENT_POID_ID0,EVENT_START_DATE,EVENT_END_DATE,EVENT_MOD_DATE,SERVICE_TYPE,EVENT_TYPE,RERATE_OBJ_ID0,BATCH_ID,EVENT_NAME,EVENT_SYS_DESCR,EVENT_RUM_NAME,PRODUCT_POID_ID0,PROD_DECSRIPTION,PRODUCT_NAME,PRODUCT_CODE,IMPACTBAL_EVENT_OBJ_ID0,IMPACT_CATEGORY,EBI_IMPACT_TYPE,EBI_AMOUNT,EBI_QUANTITY,EBI_RATE_TAG,EBI_REC_ID,EBI_RUM_ID,EBI_PRODUCT_OBJ_ID0,USAGE_RECORD_ID,DC_ID,REGION_ID,RES_ID,RES_NAME,MANAGED_FLAG,TAX_REC_ID,TAX_NAME,TAX_TYPE_ID,TAX_ELEMENT_ID,TAX_AMOUNT,TAX_RATE_PERCENT,INVOICE_SERVICEITEM_DESCR,PROD_CATEGORY,IMPACT_TYPE_DESCRIPTION,Invoice_Item_Desc,EVENT_Category,QUANTITY,UOM,RATE,AMOUNT,tblload_dtt,EBI_CURRENCY_ID,EVENT_CREATED_DATE,EBI_GL_ID,EBI_PRODUCT_OBJ_Type,EVENT_FASTLANE_SERVICE_TYPE,EVENT_FASTLANE_EVENT_TYPE,EVENT_FASTLANE_RECORD_ID,EVENT_FASTLANE_DC_ID,EVENT_FASTLANE_REGION,EVENT_FASTLANE_RESOURCE_ID,EVENT_FASTLANE_RESOURCE_NAME,EVENT_FASTLANE_ATTR1,EVENT_FASTLANE_ATTR2,EVENT_FASTLANE_ATTR3,FASTLANE_IMPACT_CATEGORY,FASTLANE_IMPACT_VALUE,EVENT_POID_TYPE,EVENT_EARNED_START_DATE,EVENT_EARNED_END_DATE,EVENT_FASTLANE_IS_BACKBILL,EBI_OFFERING_OBJ_ID0,FASTLANE_IMPACT_DEAL_CODE,FASTLANE_IMPACT_GRP_CODE,FASTLANE_IMPACT_SUB_GRP_CODE,FASTLANE_IMPACT_IS_BACKBILL,Is_Backbill,Invoice_Nk,Invoice_Source_field_NK,Invoice_Attribute_NK,invoice_attribute_source_field_nk,ACTIVITY_ATTR4,ACTIVITY_ATTR5,ACTIVITY_ATTR6,Service_Obj_Id0,ACTIVITY_ATTR7,ACTIVITY_ATTR8,Bill_Created_Date,Is_Transaction_Successful,Global_Account_Type,Item_Tag)
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
    Invoice_Item_Desc,    
    EVENT_Category,    
    QUANTITY,    
    UOM,    
    RATE,    
    AMOUNT,    
    tblload_dtt,    
    EBI_CURRENCY_ID,    
    EVENT_CREATED_DATE,        
    EBI_GL_ID,     
    EBI_PRODUCT_OBJ_Type,           
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
    EVENT_EARNED_START_DATE,      
    EVENT_EARNED_END_DATE,  
    EVENT_FASTLANE_IS_BACKBILL,   
    cast(EBI_OFFERING_OBJ_ID0 as int64) EBI_OFFERING_OBJ_ID0,    
    FASTLANE_IMPACT_DEAL_CODE,   
    FASTLANE_IMPACT_GRP_CODE,    
    FASTLANE_IMPACT_SUB_GRP_CODE,      
    FASTLANE_IMPACT_IS_BACKBILL,      
    Is_Backbill     
 ,  TO_BASE64(MD5( CONCAT( BILL_NO,'|',
    ACCOUNT_POID_ID0,'|',
	UOM,'|',
	IMPACT_CATEGORY ,'|',
	EBI_IMPACT_TYPE,'|',
	Impact_Type_Description,
	Service_Obj_Type,'|'          
	) ) ) AS Invoice_Nk                       
 ,'Bill_Number|Billing_Application_Account_Number|Unit_Measure_Of_Code|Impact_Category|Impact_Type_Id|Impact_Type_Description|Service_Type' Invoice_Source_field_NK     
 ,TO_BASE64(MD5( CONCAT(
	  CAST(COALESCE(EVENT_FASTLANE_SERVICE_TYPE,FASTLANE_IMPACT_GRP_CODE,'N/A') AS STRING) ,'|'
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
	) ) ) AS Invoice_Attribute_NK    
 ,'Invoice_Group_Code|Invoice_Sub_Group_Code|Fastlane_Data_Center_Id|Fastlane_Region|Invoice_Resource_Id|Invoice_Resource_Name|Fastlane_Attr1|Fastlane_Attr2|Fastlane_Attr3|Fastlane_Attr4|Fastlane_Attr5|Fastlane_Attr6|Fastlane_Attr7|Fastlane_Attr8|Fastlane_Impact_Category|Fastlane_Impact_Value|Deal_Code|Rate|Rate_Tag|Tax_Type_Id|Tax_Element_Id' as invoice_attribute_source_field_nk
	,ACTIVITY_ATTR4  
	,ACTIVITY_ATTR5  
	,ACTIVITY_ATTR6  
	,Service_Obj_Id0 
    ,'N/A' as  ACTIVITY_ATTR7 
	,'N/A' as  ACTIVITY_ATTR8 
	,DATE(TIMESTAMP_SECONDS(CAST(Bill_Created_Date AS INT64)))    As Bill_Created_Date  
	,0 As Is_Transaction_Successful 
	,'Cloud' AS Global_Account_Type
	,CASE 
WHEN LOWER(A.Item_Type) = '/item/adjustment' THEN 'Adjustment'
WHEN LOWER(A.Item_Type) = '/item/payment' THEN 'Payment'
WHEN LOWER(A.Item_Type) = '/item/payment/reversal' THEN 'Reversal'
WHEN LOWER(A.Item_Type) = '/item/purchase' THEN 'Purchase'
WHEN LOWER(A.Item_Type) = '/item/cycle_tax' THEN 'Tax'
ELSE NULL END AS Item_Tag
FROM Raw_InvItemEventDetail_Daily_Audit_Stage A;


END;
