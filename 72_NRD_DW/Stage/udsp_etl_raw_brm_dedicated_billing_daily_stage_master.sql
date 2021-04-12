CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_brm_dedicated_billing_daily_stage_master`()
BEGIN

--*********************************************************************************************************************  

/*

Modification log
 1) Copied SP from 72 server
 2) new field added 7.10.2019 anil2912 as per uday request for NRD Staging
 3) Added new fields (Tax_Type_Id and Tax_Element_Id) 7.23.2019 rahu4260 as per uday request for NRD staging 
 4) Added new field bill_created_date on 7.31.2019 rahu4260 as per uday request for NRD staging 

*/
create or replace temp table BRM_Dedicated_Billing_Daily_Stage_Master
as
SELECT  
    master_unique_id,  
    Account_poid,  
    parent_account_poid,  
    child_account_no,  
    BRM_ACCOUNT_NO,  
    PROFILE_POID,  
    PROFILE_TYPE,  
    ACCOUNT_OBJ_ID0,  
    ACCOUNT_NUMBER,  
    case when  lower(GL_SEGMENT) <> '.dedicated' 
	then '.dedicated' else GL_SEGMENT
	end as GL_SEGMENT,  
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
    BUSINESS_TYPE,  
    BUSINESS_TYPE_DESCR,  
    BILL_NO,  
    RAX_BILL_POID,  
    BILL_POID,  
    CURRENT_TOTAL,  
    TOTAL_DUE,  
    BILL_START_DATE,  
    BILL_END_DATE,  
    BILL_MOD_DATE,  
    ITEM_POID_ID0,  
    ITEM_NO,  
    ITEM_EFFECTIVE_DATE,  
    ITEM_MOD_DATE,  
    ITEM_NAME,  
    ITEM_STATUS,  
    ITEM_Bill_Obj_Id0,  
    SERVICE_OBJ_TYPE,  
    ITEM_TYPE,  
    EVENT_POID_ID0,  
    EVENT_Item_Obj_Id0,  
    EVENT_type,  
    EVENT_create_dtt,      EVENT_mod_dtt,  
    EVENT_start_dtt,  
    EVENT_end_dtt,  
    EVENT_descr,  
    EVENT_name,  
    EVENT_program_name,  
    EVENT_rum_name,  
    EVENT_sys_descr,  
    EVENT_usage_type,  
    EVENT_invoice_data,  
    EVENT_service_obj_id0,  
    EVENT_service_obj_type,  
    EVENT_Session_obj_ido,  
    EVENT_rerate_obj_id0,  
    BIL_CYC_resource_id,  
    BIL_CYC_quantity,  
    SERVICE_login_site_id,  
    SERVICE_obj_id0,  
    SERVICE_name,  
    service_type_classid,  
    SERVICE_Acct_poid,  
    SERVICE_BalGrp_poid,  
    SERVICE_Status,  
    SERVICE_eff_date,  
    DED_obj_id0,  
    DED_quantity,  
    DED_orig_Quantity,  
    DED_rax_uom,  
    DED_period_name,  
    DED_inv_date,  
    DED_usage_type,  
    DED_billing_type,  
    DED_device_id,  
    DED_device_name,  
    DED_bill_start,  
    DED_bill_end,  
    DED_prepay_start,  
    DED_prepay_end,  
    DED_batch_id,  
    DED_login_siteid,  
    DED_product_name,  
    DED_prod_type,  
    DED_Event_Desc,  
    DED_Make_Model,  
    DED_OS,  
    DED_RAM,  
    DED_Processor,  
    DED_record_id,  
    DED_data_center_id,  
    DED_region,  
    DED_currency_id,  
    EBI_OBJ_ID0,  
    EBI_Rec_id,  
    EBI_amount,  
    EBI_amount_Orig,  
    EBI_GL_ID,  
    EBI_impact_category,  
    EBI_impact_type,  
    EBI_product_obj_id0,  
    EBI_product_obj_type,  
    EBI_quantity,  
    EBI_rate_tag,  
    EBI_CURRENCY_ID,  
    EBI_tax_code,  
    EBI_rum_id,  
    EBI_discount,  
    EBI_offering_obj_id0,  
    EBI_offering_obj_type,  
    EBI_bal_grp_obj_ido,  
    EBI_bal_grp_obj_type,  
    MISC_EVENT_BILLING_Type,   ---Credit_Type  
    MISC_EVENT_BILLING_Type_Reason_ID,  --Credit_Reason_ID  
    MISC_EVENT_BILLING_Type_record_id,  
    string_domain,  
    string_version,       
    currency_abbrev,  
    currency_name,  
    erm_rec_id,  
    erm_rum_name,  
    product_poid,  
    product_type,  
    product_descr,  
    product_name,  
    product_code,  
    prod_permitted,  
    product_type2,  
    LINE_OF_BUSINESS,  
    EVENT_FASTLANE_SERVICE_TYPE, -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_EVENT_TYPE,  -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_RECORD_ID,  -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_DC_ID,   -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_REGION,   -- new field added 11.2.18_kvc      
    EVENT_FASTLANE_RESOURCE_ID,  -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_RESOURCE_NAME, -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_ATTR1,   -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_ATTR2,   -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_ATTR3,   -- new field added 11.2.18_kvc  
    FASTLANE_IMPACT_CATEGORY,  -- new field added 11.2.18_kvc   
    FASTLANE_IMPACT_VALUE,   -- new field added 11.2.18_kvc   
    EVENT_earned_start_dtt,   --new field added 05.8.19_kvc   
    EVENT_earned_end_dtt,   --new field added 05.8.19_kvc  
    EVENT_FASTLANE_IS_BACKBILL,  --new field added 05.8.19_kvc  
    FASTLANE_INV_DEAL_CODE,   --new field added 05.8.19_kvc  
    FASTLANE_INV_GRP_CODE,   --new field added 05.8.19_kvc  
    FASTLANE_INV_SUB_GRP_CODE,  --new field added 05.8.19_kvc  
    FASTLANE_INV_Is_Backbill,   --new field added 05.8.19_kvc 
	ACTIVITY_ATTR4,  -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR5,  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR6,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Tax_Element_Id,     --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
FROM  (
SELECT  --INTO     #BRM_Dedicated_Billing_Daily_Stage_Master    
   CAST(concat(ifnull(cast(ifnull(EVENT_POID_ID0,ITEM_POID_ID0) as string), 'X') ,'--',   
    ifnull(cast(EBI_REC_ID as string), 'X') )as string)     AS master_unique_id,  
    ACCOUNT_OBJ_ID0             AS Account_poid,  
    parent_account_poid,  
    child_account_no,  
    BRM_ACCOUNT_NO,  
    PROFILE_POID,  
    PROFILE_TYPE,  
    ACCOUNT_OBJ_ID0,  
    ACCOUNT_NUMBER,  
    DA.GL_SEGMENT,  
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
    BUSINESS_TYPE,  
    BUSINESS_TYPE_DESCR,  
    BILL_NO,  
    0       as RAX_BILL_POID,  
    BILL_POID_ID0     AS BILL_POID,  
    CURRENT_TOTAL,  
    TOTAL_DUE,  
    BILL_START_DATE,  
    BILL_END_DATE,  
    BILL_MOD_DATE,  
    ITEM_POID_ID0,  
    ITEM_NO,  
    ITEM_EFFECTIVE_DATE,  
    ITEM_MOD_DATE,  
    ITEM_NAME,  
    ITEM_STATUS,  
    ITEM_Bill_Obj_Id0,  
    SERVICE_OBJ_TYPE,  
    ITEM_TYPE,  
    EVENT_POID_ID0,  
    EVENT_Item_Obj_Id0,  
    EVENT_type,  
    EVENT_create_dtt,  
    EVENT_mod_dtt,  
    EVENT_start_dtt,  
    EVENT_end_dtt,  
    EVENT_descr,  
    EVENT_name,  
    EVENT_program_name,  
    EVENT_rum_name,  
    EVENT_sys_descr,  
    EVENT_usage_type,  
    EVENT_invoice_data,  
    EVENT_service_obj_id0,  
    EVENT_service_obj_type,  
    EVENT_Session_obj_ido,  
    EVENT_rerate_obj_id0,  
    BIL_CYC_resource_id,  
    BIL_CYC_quantity,  
    SERVICE_login_site_id,  
    SERVICE_obj_id0,  
    SERVICE_name,  
    service_type_classid,  
    SERVICE_Acct_poid,  
    SERVICE_BalGrp_poid,  
    SERVICE_Status,  
    SERVICE_eff_date,  
    DED_obj_id0,  
    DED_quantity,  
    DED_orig_Quantity,  
    DED_rax_uom,  
    DED_period_name,  
    DED_inv_date,  
    DED_usage_type,  
    DED_billing_type,  
    DED_device_id,  
    DED_device_name,  
    DED_bill_start,  
    DED_bill_end,  
    DED_prepay_start,  
    DED_prepay_end,  
    DED_batch_id,  
    DED_login_siteid,  
    DED_product_name,  
    DED_prod_type,  
    DED_Event_Desc,  
    DED_Make_Model,  
    DED_OS,  
    DED_RAM,  
    DED_Processor,  
    DED_record_id,  
    DED_data_center_id,  
    DED_region,  
    DED_currency_id,  
    EBI_OBJ_ID0,  
    EBI_Rec_id,  
    EBI_amount,  
    EBI_amount_Orig,  
    EBI_GL_ID,  
    EBI_impact_category,  
    EBI_impact_type,  
    EBI_product_obj_id0,  
    EBI_product_obj_type,  
    EBI_quantity,  
    EBI_rate_tag,  
    EBI_resource_id     AS EBI_CURRENCY_ID,  
    EBI_tax_code,  
    EBI_rum_id,  
    EBI_discount,  
    EBI_offering_obj_id0,  
    EBI_offering_obj_type,  
    EBI_bal_grp_obj_ido,  
    EBI_bal_grp_obj_type,  
    MISC_EVENT_BILLING_Type,   ---Credit_Type  
    MISC_EVENT_BILLING_Type_Reason_ID,  --Credit_Reason_ID  
    MISC_EVENT_BILLING_Type_record_id,  
    string_domain,  
    string_version,       
    currency_abbrev,  
    currency_name,  
    erm_rec_id,  
    erm_rum_name,  
    product_poid,  
    product_type,  
    product_descr,  
    product_name,  
    product_code,  
    prod_permitted,  
    product_type2,  
    DA.LINE_OF_BUSINESS,  
    ACTIVITY_SERVICE_TYPE    AS EVENT_FASTLANE_SERVICE_TYPE,  
    ACTIVITY_EVENT_TYPE     AS EVENT_FASTLANE_EVENT_TYPE,  
    ACTIVITY_RECORD_ID     AS EVENT_FASTLANE_RECORD_ID,  
    ACTIVITY_DC_ID      AS EVENT_FASTLANE_DC_ID,  
    ACTIVITY_REGION          AS EVENT_FASTLANE_REGION,  
    ACTIVITY_RESOURCE_ID         AS EVENT_FASTLANE_RESOURCE_ID,  
    ACTIVITY_RESOURCE_NAME    AS EVENT_FASTLANE_RESOURCE_NAME,  
    ACTIVITY_ATTR1      AS EVENT_FASTLANE_ATTR1,  
    ACTIVITY_ATTR2      AS EVENT_FASTLANE_ATTR2,  
    ACTIVITY_ATTR3      AS EVENT_FASTLANE_ATTR3,  
    FASTLANE_IMPACT_CATEGORY,  
    FASTLANE_IMPACT_VALUE,  
    EVENT_earned_start_dtt,   --new field added 05.8.19_kvc   
    EVENT_earned_end_dtt,   --new field added 05.8.19_kvc  
    ACTIVITY_IS_BACKBILL         AS EVENT_FASTLANE_IS_BACKBILL, --new field added 05.8.19_kvc  
    FASTLANE_INV_DEAL_CODE,   --new field added 05.8.19_kvc  
    FASTLANE_INV_GRP_CODE,   --new field added 05.8.19_kvc  
    FASTLANE_INV_SUB_GRP_CODE,  --new field added 05.8.19_kvc  
    FASTLANE_INV_Is_Backbill,   --new field added 05.8.19_kvc  
	FL.ACTIVITY_ATTR4,  -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	FL.ACTIVITY_ATTR5,  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	FL.ACTIVITY_ATTR6,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	D.Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	D.Tax_Element_Id,     --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	D.Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
	
from  
    stage_one.raw_dedicated_invoice_load_step inv --41008  
inner join  
  stage_one.raw_dedicated_event_load_step2 a  
on inv.item_poid_id0=a.event_item_obj_id0      
left outer join  
    stage_one.raw_dedicated_rax_events_load_step4  c   
on a.event_poid_id0=c.ded_obj_id0  
left outer join   
  stage_one.raw_dedicated_fast_lane fl   
on a.event_poid_id0=fl.fastlane_event_poid_id0  
left outer join   
    stage_one.raw_dedicated_credit_event_load_step b  
on a.event_poid_id0=b.misc_event_poid_id0  
left outer join  
   stage_one.raw_dedicated_impacts_load_step5 d    
on a.event_poid_id0=d.ebi_obj_id0  
left outer join   
  stage_one.raw_brm_dedicated_account_profile da   
on da.account_poid=a.event_account_poid  
where  
    ifnull(cast(ebi_amount as numeric), 0) <> 0   
AND upper(CONTACT_TYPE)='PRIMARY_CONTACT' 

union all

SELECT  --INTO     #BRM_Dedicated_Billing  
   CAST(concat(ifnull(cast(ifnull(EVENT_POID_ID0,ITEM_POID_ID0) as string), 'X') ,'--',   
    ifnull(cast(EBI_REC_ID as string), 'X')) as string)     AS master_unique_id,  
    ACCOUNT_OBJ_ID0             AS Account_poid,  
    parent_account_poid,  
    child_account_no,  
    BRM_ACCOUNT_NO,  
    PROFILE_POID,  
    PROFILE_TYPE,  
    ACCOUNT_OBJ_ID0,  
    ACCOUNT_NUMBER,  
    DA.GL_SEGMENT,  
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
    BUSINESS_TYPE,  
    BUSINESS_TYPE_DESCR,  
    BILL_NO,  
    0       as RAX_BILL_POID,  
    BILL_POID_ID0     AS BILL_POID,  
    CURRENT_TOTAL,  
    TOTAL_DUE,  
    BILL_START_DATE,  
    BILL_END_DATE,  
    BILL_MOD_DATE,  
    ITEM_POID_ID0,  
    ITEM_NO,  
    ITEM_EFFECTIVE_DATE,  
    ITEM_MOD_DATE,  
    ITEM_NAME,  
    ITEM_STATUS,  
    ITEM_Bill_Obj_Id0,  
    SERVICE_OBJ_TYPE,  
    ITEM_TYPE,  
    EVENT_POID_ID0,  
    EVENT_Item_Obj_Id0,  
    EVENT_type,  
    EVENT_create_dtt,  
    EVENT_mod_dtt,  
    EVENT_start_dtt,  
    EVENT_end_dtt,  
    EVENT_descr,  
    EVENT_name,  
    EVENT_program_name,  
    EVENT_rum_name,  
    EVENT_sys_descr,  
    EVENT_usage_type,  
    EVENT_invoice_data,  
    EVENT_service_obj_id0,  
    EVENT_service_obj_type,  
    EVENT_Session_obj_ido,  
    EVENT_rerate_obj_id0,  
    BIL_CYC_resource_id,  
    BIL_CYC_quantity,  
    SERVICE_login_site_id,  
    SERVICE_obj_id0,  
    SERVICE_name,  
    service_type_classid,  
    SERVICE_Acct_poid,  
    SERVICE_BalGrp_poid,  
    SERVICE_Status,  
    SERVICE_eff_date,  
    DED_obj_id0,  
    DED_quantity,  
    DED_orig_Quantity,  
    DED_rax_uom,  
    DED_period_name,  
    DED_inv_date,  
    DED_usage_type,  
    DED_billing_type,  
    DED_device_id,  
    DED_device_name,  
    DED_bill_start,  
    DED_bill_end,  
    DED_prepay_start,  
    DED_prepay_end,  
    DED_batch_id,  
    DED_login_siteid,  
    DED_product_name,  
    DED_prod_type,  
    DED_Event_Desc,  
    DED_Make_Model,  
    DED_OS,  
    DED_RAM,  
    DED_Processor,  
    DED_record_id,  
    DED_data_center_id,  
    DED_region,  
    DED_currency_id,  
    EBI_OBJ_ID0,  
    EBI_Rec_id,  
    EBI_amount,  
    EBI_amount_Orig,  
    EBI_GL_ID,  
    EBI_impact_category,  
    EBI_impact_type,  
    EBI_product_obj_id0,  
    EBI_product_obj_type,  
    EBI_quantity,  
    EBI_rate_tag,  
    EBI_resource_id     AS EBI_CURRENCY_ID,  
    EBI_tax_code,  
    EBI_rum_id,  
    EBI_discount,  
    EBI_offering_obj_id0,  
    EBI_offering_obj_type,  
    EBI_bal_grp_obj_ido,  
    EBI_bal_grp_obj_type,  
    MISC_EVENT_BILLING_Type,   ---Credit_Type  
    MISC_EVENT_BILLING_Type_Reason_ID,  --Credit_Reason_ID  
    MISC_EVENT_BILLING_Type_record_id,  
    string_domain,  
    string_version,       
    currency_abbrev,  
    currency_name,  
    erm_rec_id,  
    erm_rum_name,  
    product_poid,  
    product_type,  
    product_descr,  
    product_name,  
    product_code,  
    prod_permitted,  
    product_type2,  
    DA.LINE_OF_BUSINESS,  
    ACTIVITY_SERVICE_TYPE    AS EVENT_FASTLANE_SERVICE_TYPE,  
    ACTIVITY_EVENT_TYPE     AS EVENT_FASTLANE_EVENT_TYPE,  
    ACTIVITY_RECORD_ID     AS EVENT_FASTLANE_RECORD_ID,  
    ACTIVITY_DC_ID      AS EVENT_FASTLANE_DC_ID,  
    ACTIVITY_REGION          AS EVENT_FASTLANE_REGION,  
    ACTIVITY_RESOURCE_ID         AS EVENT_FASTLANE_RESOURCE_ID,  
    ACTIVITY_RESOURCE_NAME    AS EVENT_FASTLANE_RESOURCE_NAME,  
    ACTIVITY_ATTR1      AS EVENT_FASTLANE_ATTR1,  
    ACTIVITY_ATTR2      AS EVENT_FASTLANE_ATTR2,  
    ACTIVITY_ATTR3      AS EVENT_FASTLANE_ATTR3,  
    FASTLANE_IMPACT_CATEGORY,  
    FASTLANE_IMPACT_VALUE,  
    EVENT_earned_start_dtt,   --new field added 05.8.19_kvc   
    EVENT_earned_end_dtt,   --new field added 05.8.19_kvc  
    ACTIVITY_IS_BACKBILL         AS EVENT_FASTLANE_IS_BACKBILL, --new field added 05.8.19_kvc  
    FASTLANE_INV_DEAL_CODE,   --new field added 05.8.19_kvc  
    FASTLANE_INV_GRP_CODE,   --new field added 05.8.19_kvc  
    FASTLANE_INV_SUB_GRP_CODE,  --new field added 05.8.19_kvc  
    FASTLANE_INV_Is_Backbill,   --new field added 05.8.19_kvc  
	FL.ACTIVITY_ATTR4,  -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	FL.ACTIVITY_ATTR5,  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	FL.ACTIVITY_ATTR6,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	D.Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	D.Tax_Element_Id,     --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	D.Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
From  
    stage_one.raw_dedicated_invoice_load_step inv --41008  
inner join  
    stage_one.raw_dedicated_event_load_step2 a  
on inv.item_poid_id0=a.event_item_obj_id0      
left outer join  
    stage_one.raw_dedicated_rax_events_load_step4  c   
on a.event_poid_id0=c.ded_obj_id0  
left outer join  
    stage_one.raw_dedicated_impacts_load_step5 d    
on a.event_poid_id0=d.ebi_obj_id0  
left outer join   
    stage_one.raw_dedicated_fast_lane fl   
on a.event_poid_id0=fl.fastlane_event_poid_id0  
left outer join   
    stage_one.raw_dedicated_credit_event_load_step b  
on a.event_poid_id0=b.misc_event_poid_id0  
left outer join   
    stage_one.raw_brm_dedicated_account_profile da   
on da.account_poid=a.event_account_poid  
WHERE  
    ifnull(CAST(EBI_amount AS numeric), 0) <> 0   
AND upper(CONTACT_TYPE)='BILLING'
); 

create or replace temp table dupes as 
SELECT   *   --INTO     #dupes  
FROM    
    BRM_Dedicated_Billing_Daily_Stage_Master   
WHERE  master_unique_id IN (
							SELECT  master_unique_id  
							FROM  
								(  
								SELECT master_unique_id, COUNT(master_unique_id) AS count_master_unique_id  
								FROM BRM_Dedicated_Billing_Daily_Stage_Master  
								GROUP BY  
									master_unique_id  
								HAVING  COUNT(master_unique_id) >1  
								)A  
							)  ;
--*********************************************************************************************************************   
DELETE FROM  BRM_Dedicated_Billing_Daily_Stage_Master  
WHERE master_unique_id IN  (SELECT master_unique_id FROM  dupes)  ;
--*********************************************************************************************************************   
INSERT INTO BRM_Dedicated_Billing_Daily_Stage_Master  
SELECT * FROM  dupes WHERE lower(string_domain) LIKE 'reason%'  ;

   
INSERT INTO  stage_one.raw_brm_dedicated_billing_daily_stage_master 
(master_unique_id,Account_poid,parent_account_poid,child_account_no,BRM_ACCOUNT_NO,PROFILE_POID,PROFILE_TYPE,ACCOUNT_OBJ_ID0,ACCOUNT_NUMBER,GL_SEGMENT,CUSTOMER_TYPE,CUSTOMER_TYPE_DESCR,BILLING_SEGMENT,BILLING_SEG_DESCR,CONTRACTING_ENTITY,SUPPORT_TEAM,SUPPORT_TEAM_DESCR,team_customer_type,team_cust_class,ORG_VALUE,ORGANIZATION,BUSINESS_UNIT,PAYMENT_TERM,BUSINESS_TYPE,BUSINESS_TYPE_DESCR,BILL_NO,RAX_BILL_POID,BILL_POID,CURRENT_TOTAL,TOTAL_DUE,BILL_START_DATE,BILL_END_DATE,BILL_MOD_DATE,ITEM_POID_ID0,ITEM_NO,ITEM_EFFECTIVE_DATE,ITEM_MOD_DATE,ITEM_NAME,ITEM_STATUS,SERVICE_OBJ_TYPE,ITEM_TYPE,ITEM_Bill_Obj_Id0,EVENT_POID_ID0,EVENT_Item_Obj_Id0,EVENT_type,EVENT_create_dtt,EVENT_mod_dtt,EVENT_start_dtt,EVENT_end_dtt,EVENT_descr,EVENT_name,EVENT_program_name,EVENT_rum_name,EVENT_sys_descr,EVENT_usage_type,EVENT_invoice_data,EVENT_service_obj_id0,EVENT_service_obj_type,EVENT_Session_obj_ido,EVENT_rerate_obj_id0,BIL_CYC_resource_id,BIL_CYC_quantity,SERVICE_login_site_id,SERVICE_obj_id0,SERVICE_name,service_type_classid,SERVICE_Acct_poid,SERVICE_BalGrp_poid,SERVICE_Status,SERVICE_eff_date,DED_obj_id0,DED_quantity,DED_orig_Quantity,DED_rax_uom,DED_period_name,DED_inv_date,DED_usage_type,DED_billing_type,DED_device_id,DED_device_name,DED_bill_start,DED_bill_end,DED_prepay_start,DED_prepay_end,DED_batch_id,DED_login_siteid,DED_product_name,DED_prod_type,DED_Event_Desc,DED_Make_Model,DED_OS,DED_RAM,DED_Processor,DED_record_id,DED_data_center_id,DED_region,DED_currency_id,EBI_OBJ_ID0,EBI_Rec_id,EBI_amount,EBI_amount_Orig,EBI_GL_ID,EBI_impact_category,EBI_impact_type,EBI_product_obj_id0,EBI_product_obj_type,EBI_quantity,EBI_rate_tag,EBI_CURRENCY_ID,EBI_tax_code,EBI_rum_id,EBI_discount,EBI_offering_obj_id0,EBI_offering_obj_type,EBI_bal_grp_obj_ido,EBI_bal_grp_obj_type,MISC_EVENT_BILLING_Type,MISC_EVENT_BILLING_Type_Reason_ID,MISC_EVENT_BILLING_Type_record_id,string_domain,string_version,currency_abbrev,currency_name,erm_rec_id,erm_rum_name,product_poid,product_type,product_descr,product_name,product_code,prod_permitted,product_type2,master_tbl_loaddtt,LINE_OF_BUSINESS,EVENT_FASTLANE_SERVICE_TYPE,EVENT_FASTLANE_EVENT_TYPE,EVENT_FASTLANE_RECORD_ID,EVENT_FASTLANE_DC_ID,EVENT_FASTLANE_REGION,EVENT_FASTLANE_RESOURCE_ID,EVENT_FASTLANE_RESOURCE_NAME,EVENT_FASTLANE_ATTR1,EVENT_FASTLANE_ATTR2,EVENT_FASTLANE_ATTR3,FASTLANE_IMPACT_CATEGORY,FASTLANE_IMPACT_VALUE,EVENT_earned_start_dtt,EVENT_earned_end_dtt,EVENT_FASTLANE_IS_BACKBILL,FASTLANE_INV_DEAL_CODE,FASTLANE_INV_GRP_CODE,FASTLANE_INV_SUB_GRP_CODE,FASTLANE_INV_Is_Backbill,ACTIVITY_ATTR4,ACTIVITY_ATTR5,ACTIVITY_ATTR6,Tax_Type_Id,Tax_Element_Id,Bill_Created_Date)
SELECT  
    master_unique_id,  
    Account_poid,  
    parent_account_poid,  
    child_account_no,  
    BRM_ACCOUNT_NO,  
    PROFILE_POID,  
    PROFILE_TYPE,  
    ACCOUNT_OBJ_ID0,  
    ACCOUNT_NUMBER,  
    GL_SEGMENT,  
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
    BUSINESS_TYPE,  
    BUSINESS_TYPE_DESCR,  
    BILL_NO,  
    RAX_BILL_POID,  
    BILL_POID,  
    cast(CURRENT_TOTAL as numeric) as CURRENT_TOTAL,  
    cast(TOTAL_DUE as numeric) as TOTAL_DUE,  
    cast(BILL_START_DATE as date) as BILL_START_DATE,  
    cast(BILL_END_DATE as date) as BILL_END_DATE,  
    cast(BILL_MOD_DATE as date) as BILL_MOD_DATE,  
    ITEM_POID_ID0,  
    ITEM_NO,  
    cast(ITEM_EFFECTIVE_DATE as date) as ITEM_EFFECTIVE_DATE,  
    cast(ITEM_MOD_DATE as date) as ITEM_MOD_DATE,  
    ITEM_NAME,  
    ITEM_STATUS,  
    SERVICE_OBJ_TYPE,  
    ITEM_TYPE,  
    ITEM_Bill_Obj_Id0,  
    EVENT_POID_ID0,  
    EVENT_Item_Obj_Id0,  
    EVENT_type,  
    EVENT_create_dtt,  
    EVENT_mod_dtt,  
    EVENT_start_dtt,  
    EVENT_end_dtt,  
    EVENT_descr,  
    EVENT_name,  
    EVENT_program_name,  
    EVENT_rum_name,  
    EVENT_sys_descr,  
    EVENT_usage_type,  
    EVENT_invoice_data,  
    EVENT_service_obj_id0,  
    EVENT_service_obj_type,  
    EVENT_Session_obj_ido,  
    EVENT_rerate_obj_id0,  
    BIL_CYC_resource_id,  
    BIL_CYC_quantity,  
    SERVICE_login_site_id,  
    SERVICE_obj_id0,  
    SERVICE_name,  
    service_type_classid,  
    SERVICE_Acct_poid,  
    SERVICE_BalGrp_poid,  
    SERVICE_Status,  
    SERVICE_eff_date,  
    DED_obj_id0,  
    DED_quantity,  
    DED_orig_Quantity,  
    DED_rax_uom,  
    DED_period_name,  
    DED_inv_date,  
    DED_usage_type,  
    DED_billing_type,  
    DED_device_id,  
    DED_device_name,  
    DED_bill_start,  
    DED_bill_end,  
    DED_prepay_start,  
    DED_prepay_end,  
    DED_batch_id,  
    DED_login_siteid,  
    DED_product_name,  
    DED_prod_type,  
    DED_Event_Desc,  
    DED_Make_Model,  
    DED_OS,  
    DED_RAM,  
    DED_Processor,  
    DED_record_id,  
    DED_data_center_id,  
    DED_region,  
    DED_currency_id,  
    EBI_OBJ_ID0,  
    EBI_Rec_id,  
    EBI_amount,  
    EBI_amount_Orig,  
    EBI_GL_ID,  
    EBI_impact_category,  
    EBI_impact_type,  
    EBI_product_obj_id0,  
    EBI_product_obj_type,  
    EBI_quantity,  
    EBI_rate_tag,  
    EBI_CURRENCY_ID,  
    EBI_tax_code,  
    EBI_rum_id,  
    EBI_discount,  
    EBI_offering_obj_id0,  
    EBI_offering_obj_type,  
    EBI_bal_grp_obj_ido,  
    EBI_bal_grp_obj_type,  
    MISC_EVENT_BILLING_Type,   ---Credit_Type  
    MISC_EVENT_BILLING_Type_Reason_ID,  --Credit_Reason_ID  
    MISC_EVENT_BILLING_Type_record_id,  
    string_domain,  
    string_version,       
    currency_abbrev,  
    currency_name,  
    erm_rec_id,  
    erm_rum_name,  
    product_poid,  
    product_type,  
    product_descr,  
    product_name,  
    product_code,  
    prod_permitted,  
    product_type2,  
    current_datetime()      AS master_tbl_loaddtt,  
    LINE_OF_BUSINESS,  
    EVENT_FASTLANE_SERVICE_TYPE, -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_EVENT_TYPE,  -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_RECORD_ID,  -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_DC_ID,   -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_REGION,   -- new field added 11.2.18_kvc      
    EVENT_FASTLANE_RESOURCE_ID,  -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_RESOURCE_NAME, -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_ATTR1,   -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_ATTR2,   -- new field added 11.2.18_kvc  
    EVENT_FASTLANE_ATTR3,   -- new field added 11.2.18_kvc  
    FASTLANE_IMPACT_CATEGORY,  -- new field added 11.2.18_kvc   
    FASTLANE_IMPACT_VALUE,   -- new field added 11.2.18_kvc   
    EVENT_earned_start_dtt,   --new field added 05.8.19_kvc   
    EVENT_earned_end_dtt,   --new field added 05.8.19_kvc  
    EVENT_FASTLANE_IS_BACKBILL,  --new field added 05.8.19_kvc  
    FASTLANE_INV_DEAL_CODE,   --new field added 05.8.19_kvc  
    FASTLANE_INV_GRP_CODE,   --new field added 05.8.19_kvc  
    FASTLANE_INV_SUB_GRP_CODE,  --new field added 05.8.19_kvc  
    FASTLANE_INV_Is_Backbill,   --new field added 05.8.19_kvc  
	cast(ACTIVITY_ATTR4  as string) as ACTIVITY_ATTR4,  -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	cast(ACTIVITY_ATTR5 as string) as ACTIVITY_ATTR5,  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	cast(ACTIVITY_ATTR6 as string) as ACTIVITY_ATTR6,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	cast(Tax_Type_Id  as string) as Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	cast(Tax_Element_Id  as string) as Tax_Element_Id,     --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
FROM  
   BRM_Dedicated_Billing_Daily_Stage_Master  ;
END;
