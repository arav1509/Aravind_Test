CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_brm_dedicated_billing_daily_stage_master_audit`()
BEGIN



-------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE raw_brm_dedicated_invoice_aggregate_total_audit AS
select distinct
    bill_poid_id0,
    bill_no,
    sum(current_total)     as current_total,
    sum(total_due)         as total_due,
    max(bill_start_date)       as bill_start_date,
    max(bill_end_date)     as bill_end_date,
    max(bill_mod_date)     as bill_mod_date
from
    stage_one.raw_brm_dedicated_invoice_aggregate_total
group by
     bill_poid_id0,
    bill_no;
/*--[Ashok: Commented belwo create INDEX]
--********************************************************************************************************************* 
create index ix_BILL_POID_ID0 on #raw_brm_dedicated_invoice_aggregate_total_audit (BILL_POID_ID0)
create index ix_BILL_NO on #raw_brm_dedicated_invoice_aggregate_total_audit (BILL_NO)
--*********************************************************************************************************************
*/
CREATE OR REPLACE TEMP TABLE  raw_brm_dedicated_billing_daily_master_audit AS  
(
select distinct
    cast(CONCAT(COALESCE(cast(COALESCE(b.event_poid_id0,item_poid_id0) as string), 'X') ,'--', 
    COALESCE(cast(ebi_rec_id as string), 'X')) as string)            as master_unique_id,
    COALESCE(da.account_obj_id0, da2.account_poid)                   as account_poid,
    COALESCE(da.parent_account_poid,da2.parent_account_poid)                 as parent_account_poid,
    COALESCE(da.child_account_no,da2.child_account_no)                   as child_account_no,
    COALESCE(da.brm_account_no,da2.brm_account_no)                       as brm_account_no,
    COALESCE(da.profile_poid,da2.profile_poid)                           as profile_poid,
    COALESCE(da.profile_type,da2.profile_type)                           as profile_type,
    COALESCE(da.account_obj_id0,da2.account_poid)                        as account_obj_id0,
    COALESCE(da.account_number,da2.account_number)                       as account_number,
    COALESCE(da.gl_segment,da2.gl_segment)                           as gl_segment,
    COALESCE(da.customer_type,da2.customer_type)                         as customer_type,
    COALESCE(da.customer_type_descr,da2.customer_type_descr)                 as customer_type_descr,
    COALESCE(da.billing_segment,da2.billing_segment)                     as billing_segment,
    COALESCE(da.billing_seg_descr,da2.billing_seg_descr)                     as billing_seg_descr,
    COALESCE(da.contracting_entity,da2.contracting_entity)               as contracting_entity,
    COALESCE(da.support_team,da2.support_team)                           as support_team,
    COALESCE(da.support_team_descr,da2.support_team_descr)               as support_team_descr,
    COALESCE(da.team_customer_type,da2.team_customer_type)               as team_customer_type,
    COALESCE(da.team_cust_class,da2.team_cust_class)                     as team_cust_class,
    COALESCE(da.org_value,da2.org_value)                                 as org_value,
    COALESCE(da.organization,da2.organization)                           as organization,
    COALESCE(da.business_unit,da2.business_unit)                         as business_unit,
    COALESCE(da.payment_term,da2.payment_term)                           as payment_term,
    COALESCE(da.business_type,da2.business_type)                         as business_type,
    COALESCE(da.business_type_descr,da2.business_type_descr)                 as business_type_descr,
    e.bill_no,
    0                                                        as rax_bill_poid,
    bill_poid_id0                                                as bill_poid,
    current_total,
    total_due,
    bill_start_date,
    bill_end_date,
    bill_mod_date,
    item_poid_id0,
    item_no,
    item_effective_date,
    item_mod_date,
    item_name,
    item_status,
    item_bill_obj_id0,
    service_obj_type,
    item_type,
    b.event_poid_id0,
    event_item_obj_id0,
    event_type,
    event_create_dtt,
    event_mod_dtt,
    event_start_dtt,
    event_end_dtt,
    event_descr,
    event_name,
    event_program_name,
    event_rum_name,
    event_sys_descr,
    event_usage_type,
    event_invoice_data,
    event_service_obj_id0,
    event_service_obj_type,
    event_session_obj_ido,
    event_rerate_obj_id0,
    bil_cyc_resource_id,
    bil_cyc_quantity,
    service_login_site_id,
    service_obj_id0,
    service_name,
    service_type_classid,
    service_acct_poid,
    service_balgrp_poid,
    service_status,
    service_eff_date,
    ded_obj_id0,
    ded_quantity,
    ded_orig_quantity,
    ded_rax_uom,
    ded_period_name,
    ded_inv_date,
    ded_usage_type,
    ded_billing_type,
    ded_device_id,
    ded_device_name,
    ded_bill_start,
    ded_bill_end,
    ded_prepay_start,
    ded_prepay_end,
    ded_batch_id,
    ded_login_siteid,
    ded_product_name,
    ded_prod_type,
    ded_event_desc,
    ded_make_model,
    ded_os,
    ded_ram,
    ded_processor,
    ded_record_id,
    ded_data_center_id,
    ded_region,
    ded_currency_id,
    ebi_obj_id0,
    ebi_rec_id,
    ebi_amount                      as ebi_amount,
    ebi_amount_orig,
    ebi_gl_id,
    ebi_impact_category,
    ebi_impact_type,
    ebi_product_obj_id0,
    ebi_product_obj_type,
    ebi_quantity,
    ebi_rate_tag,
    ebi_resource_id                 as ebi_currency_id,
    ebi_tax_code,
    ebi_rum_id,
    ebi_discount,
    ebi_offering_obj_id0,
    ebi_offering_obj_type,
    ebi_bal_grp_obj_ido,
    ebi_bal_grp_obj_type,
    misc_event_billing_type,            ---credit_type
    misc_event_billing_type_reason_id,  --credit_reason_id
    misc_event_billing_type_record_id,
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
    COALESCE(da.line_of_business,da2.line_of_business)               as line_of_business,
    activity_service_type               as event_fastlane_service_type,
    activity_event_type                 as event_fastlane_event_type,
    activity_record_id                  as event_fastlane_record_id,
    activity_dc_id                      as event_fastlane_dc_id,
    activity_region                      as event_fastlane_region,
    activity_resource_id                     as event_fastlane_resource_id,
    activity_resource_name              as event_fastlane_resource_name,
    activity_attr1                      as event_fastlane_attr1,
    activity_attr2                      as event_fastlane_attr2,
    activity_attr3                      as event_fastlane_attr3,
    fastlane_impact_category,
    fastlane_impact_value,          -- new field added 11.2.18_kvc 
    event_earned_start_dtt,         --new field added 05.8.19_kvc 
    event_earned_end_dtt,           --new field added 05.8.19_kvc
    ACTIVITY_backbill_flag                    as event_fastlane_is_backbill, --new field added 05.8.19_kvc
    fastlane_inv_deal_code,         --new field added 05.8.19_kvc
    fastlane_inv_grp_code,          --new field added 05.8.19_kvc
    fastlane_inv_sub_grp_code,      --new field added 05.8.19_kvc
    fastlane_inv_is_backbill,            --new field added 05.8.19_kvc
	FL.ACTIVITY_ATTR4,  -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	FL.ACTIVITY_ATTR5,  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	FL.ACTIVITY_ATTR6,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	D.Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	D.Tax_Element_Id,     --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	D.Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging

from
    stage_one.raw_dedicated_item_audit_step1 a-- stage_dedicated_event_load_step2 a'
inner join
    stage_one.raw_dedicated_event_audit_step3 b
on a.item_poid_id0=b.event_item_obj_id0
left outer join
    stage_one.raw_dedicated_adj_audit_step2 cc
on b.event_poid_id0=cc.event_poid_id0  
left outer join
    stage_one.raw_dedicated_rax_events_audit_load_step4  c 
on b.event_poid_id0=c.ded_obj_id0
left outer join
   stage_one.raw_dedicated_impacts_audit_load_step6 d  
on b.event_poid_id0=d.ebi_obj_id0
left outer join 
  stage_one.raw_dedicated_audit_fast_lane fl 
on b.event_poid_id0=fl.fastlane_event_poid_id0
left outer join 
    raw_brm_dedicated_invoice_aggregate_total_audit e 
on a.item_bill_obj_id0=e.bill_poid_id0
left outer join 
    stage_one.raw_brm_dedicated_account_profile da 
on da.account_poid=event_account_poid
left outer join 
    stage_one.raw_brm_dedicated_account_profile da2 
on da2.account_poid=a.account_obj_id0
where
    (
    lower(da.contact_type) ='primary_contact'
and lower(da2.contact_type) ='primary_contact'
)
and COALESCE(ebi_amount,0) <> 0);
--[Ashok: Commented below create index]
--********************************************************************************************************************* 
--create index ix_master_unique_id on #BRM_Dedicated_Billing_Daily_Stage_Master_Audit (master_unique_id)
--*********************************************************************************************************************

CREATE OR REPLACE TEMP TABLE brm_dedicated_billing2 AS (
select distinct
   cast(CONCAT(COALESCE(cast(COALESCE(b.event_poid_id0,item_poid_id0) as string), 'X') ,'--', 
    COALESCE(cast(ebi_rec_id as string), 'X')) as string)            as master_unique_id,
    COALESCE(da.account_obj_id0, da2.account_poid)                   as account_poid,
    COALESCE(da.parent_account_poid,da2.parent_account_poid)                 as parent_account_poid,
    COALESCE(da.child_account_no,da2.child_account_no)                   as child_account_no,
    COALESCE(da.brm_account_no,da2.brm_account_no)                       as brm_account_no,
    COALESCE(da.profile_poid,da2.profile_poid)                           as profile_poid,
    COALESCE(da.profile_type,da2.profile_type)                           as profile_type,
    COALESCE(da.account_obj_id0, da2.account_poid)                       as account_obj_id0,
    COALESCE(da.account_number,da2.account_number)                       as account_number,
    COALESCE(da.gl_segment,da2.gl_segment)                           as gl_segment,
    COALESCE(da.customer_type,da2.customer_type)                         as customer_type,
    COALESCE(da.customer_type_descr,da2.customer_type_descr)                 as customer_type_descr,
    COALESCE(da.billing_segment,da2.billing_segment)                     as billing_segment,
    COALESCE(da.billing_seg_descr,da2.billing_seg_descr)                     as billing_seg_descr,
    COALESCE(da.contracting_entity,da2.contracting_entity)               as contracting_entity,
    COALESCE(da.support_team,da2.support_team)                           as support_team,
    COALESCE(da.support_team_descr,da2.support_team_descr)               as support_team_descr,
    COALESCE(da.team_customer_type,da2.team_customer_type)               as team_customer_type,
    COALESCE(da.team_cust_class,da2.team_cust_class)                     as team_cust_class,
    COALESCE(da.org_value,da2.org_value)                                 as org_value,
    COALESCE(da.organization,da2.organization)                           as organization,
    COALESCE(da.business_unit,da2.business_unit)                         as business_unit,
    COALESCE(da.payment_term,da2.payment_term)                           as payment_term,
    COALESCE(da.business_type,da2.business_type)                         as business_type,
    COALESCE(da.business_type_descr,da2.business_type_descr)                 as business_type_descr,
    e.bill_no,
    0                                                        as rax_bill_poid,
    bill_poid_id0                                                as bill_poid,
    current_total,
    total_due,
    bill_start_date,
    bill_end_date,
    bill_mod_date,
    item_poid_id0,
    item_no,
    item_effective_date,
    item_mod_date,
    item_name,
    item_status,
    item_bill_obj_id0,
    service_obj_type,
    item_type,
    b.event_poid_id0,
    event_item_obj_id0,
    event_type,
    event_create_dtt,
    event_mod_dtt,
    event_start_dtt,
    event_end_dtt,
    event_descr,
    event_name,
    event_program_name,
    event_rum_name,
    event_sys_descr,
    event_usage_type,
    event_invoice_data,
    event_service_obj_id0,
    event_service_obj_type,
    event_session_obj_ido,
    event_rerate_obj_id0,
    bil_cyc_resource_id,
    bil_cyc_quantity,
    service_login_site_id,
    service_obj_id0,
    service_name,
    service_type_classid,
    service_acct_poid,
    service_balgrp_poid,
    service_status,
    service_eff_date,
    ded_obj_id0,
    ded_quantity,
    ded_orig_quantity,
    ded_rax_uom,
    ded_period_name,
    ded_inv_date,
    ded_usage_type,
    ded_billing_type,
    ded_device_id,
    ded_device_name,
    ded_bill_start,
    ded_bill_end,
    ded_prepay_start,
    ded_prepay_end,
    ded_batch_id,
    ded_login_siteid,
    ded_product_name,
    ded_prod_type,
    ded_event_desc,
    ded_make_model,
    ded_os,
    ded_ram,
    ded_processor,
    ded_record_id,
    ded_data_center_id,
    ded_region,
    ded_currency_id,
    ebi_obj_id0,
    ebi_rec_id,
    ebi_amount                      as ebi_amount,
    ebi_amount_orig,
    ebi_gl_id,
    ebi_impact_category,
    ebi_impact_type,
    ebi_product_obj_id0,
    ebi_product_obj_type,
    ebi_quantity,
    ebi_rate_tag,
    ebi_resource_id                 as ebi_currency_id,
    ebi_tax_code,
    ebi_rum_id,
    ebi_discount,
    ebi_offering_obj_id0,
    ebi_offering_obj_type,
    ebi_bal_grp_obj_ido,
    ebi_bal_grp_obj_type,
    misc_event_billing_type,            ---credit_type
    misc_event_billing_type_reason_id,  --credit_reason_id
    misc_event_billing_type_record_id,
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
    COALESCE(da.line_of_business,da2.line_of_business)               as line_of_business,
    activity_service_type               as event_fastlane_service_type,
    activity_event_type                 as event_fastlane_event_type,
    activity_record_id                  as event_fastlane_record_id,
    activity_dc_id                      as event_fastlane_dc_id,
    activity_region                      as event_fastlane_region,
    activity_resource_id                     as event_fastlane_resource_id,
    activity_resource_name              as event_fastlane_resource_name,
    activity_attr1                      as event_fastlane_attr1,
    activity_attr2                      as event_fastlane_attr2,
    activity_attr3                      as event_fastlane_attr3,
    fastlane_impact_category,           -- new field added 11.2.18_kvc 
    fastlane_impact_value,          -- new field added 11.2.18_kvc 
    event_earned_start_dtt,         --new field added 05.8.19_kvc 
    event_earned_end_dtt,           --new field added 05.8.19_kvc
    ACTIVITY_backbill_flag                     as event_fastlane_is_backbill, --new field added 05.8.19_kvc
    fastlane_inv_deal_code,         --new field added 05.8.19_kvc
    fastlane_inv_grp_code,          --new field added 05.8.19_kvc
    fastlane_inv_sub_grp_code,      --new field added 05.8.19_kvc
    fastlane_inv_is_backbill   ,         --new field added 05.8.19_kvc
	FL.ACTIVITY_ATTR4,  -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	FL.ACTIVITY_ATTR5,  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	FL.ACTIVITY_ATTR6,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	D.Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	D.Tax_Element_Id,     --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	D.Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
from
  stage_one.raw_dedicated_item_audit_step1 a-- dbo.stage_dedicated_event_load_step2 a'
inner join
    stage_one.raw_dedicated_event_audit_step3 b
on a.item_poid_id0=b.event_item_obj_id0
left outer join
    stage_one.raw_dedicated_adj_audit_step2 cc
on b.event_poid_id0=cc.event_poid_id0  
left outer join
    stage_one.raw_dedicated_rax_events_audit_load_step4  c 
on b.event_poid_id0=c.ded_obj_id0
left outer join
   stage_one.raw_dedicated_impacts_audit_load_step6 d  
on b.event_poid_id0=d.ebi_obj_id0
left outer join 
  stage_one.raw_dedicated_audit_fast_lane fl 
on b.event_poid_id0=fl.fastlane_event_poid_id0
left outer join 
    raw_brm_dedicated_invoice_aggregate_total_audit e 
on a.item_bill_obj_id0=e.bill_poid_id0
left outer join 
    stage_one.raw_brm_dedicated_account_profile da 
on da.account_poid=event_account_poid
left outer join 
    stage_one.raw_brm_dedicated_account_profile da2 
on da2.account_poid=a.account_obj_id0
where
    (
    lower(da.contact_type) ='billing'
or lower(da2.contact_type) ='billing'
)
and COALESCE(ebi_amount,0) <> 0
);

/*[Ashok: Commented below CREATE INDEX]
********************************************************************************************************************* 
create index ix_master_unique_id on #BRM_Dedicated_Billing2 (master_unique_id)
*********************************************************************************************************************
*/
INSERT INTO  raw_brm_dedicated_billing_daily_master_audit
(
select distinct
    master_unique_id,
    account_poid,
    parent_account_poid,
    child_account_no,
    brm_account_no,
    profile_poid,
    profile_type,
    account_obj_id0,
    account_number,
    gl_segment,
    customer_type,
    customer_type_descr,
    billing_segment,
    billing_seg_descr,
    contracting_entity,
    support_team,
    support_team_descr,
    team_customer_type,
    team_cust_class,
    org_value,
    organization,
    business_unit,
    payment_term,
    business_type,
    business_type_descr,
    bill_no,
    rax_bill_poid,
    bill_poid,
    current_total,
    total_due,
    bill_start_date,
    bill_end_date,
    bill_mod_date,
    item_poid_id0,
    item_no,
    item_effective_date,
    item_mod_date,
    item_name,
    item_status,
    item_bill_obj_id0,
    service_obj_type,
    item_type,
    event_poid_id0,
    event_item_obj_id0,
    event_type,
    event_create_dtt,
    event_mod_dtt,
    event_start_dtt,
    event_end_dtt,
    event_descr,
    event_name,
    event_program_name,
    event_rum_name,
    event_sys_descr,
    event_usage_type,
    event_invoice_data,
    event_service_obj_id0,
    event_service_obj_type,
    event_session_obj_ido,
    event_rerate_obj_id0,
    bil_cyc_resource_id,
    bil_cyc_quantity,
    service_login_site_id,
    service_obj_id0,
    service_name,
    service_type_classid,
    service_acct_poid,
    service_balgrp_poid,
    service_status,
    service_eff_date,
    ded_obj_id0,
    ded_quantity,
    ded_orig_quantity,
    ded_rax_uom,
    ded_period_name,
    ded_inv_date,
    ded_usage_type,
    ded_billing_type,
    ded_device_id,
    ded_device_name,
    ded_bill_start,
    ded_bill_end,
    ded_prepay_start,
    ded_prepay_end,
    ded_batch_id,
    ded_login_siteid,
    ded_product_name,
    ded_prod_type,
    ded_event_desc,
    ded_make_model,
    ded_os,
    ded_ram,
    ded_processor,
    ded_record_id,
    ded_data_center_id,
    ded_region,
    ded_currency_id,
    ebi_obj_id0,
    ebi_rec_id,
    ebi_amount,
    ebi_amount_orig,
    ebi_gl_id,
    ebi_impact_category,
    ebi_impact_type,
    ebi_product_obj_id0,
    ebi_product_obj_type,
    ebi_quantity,
    ebi_rate_tag,
    ebi_currency_id,
    ebi_tax_code,
    ebi_rum_id,
    ebi_discount,
    ebi_offering_obj_id0,
    ebi_offering_obj_type,
    ebi_bal_grp_obj_ido,
    ebi_bal_grp_obj_type,
    misc_event_billing_type,            ---credit_type
    misc_event_billing_type_reason_id,  --credit_reason_id
    misc_event_billing_type_record_id,
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
   -- CURRENT_DATETIME()                           as master_tbl_loaddtt,
    line_of_business,
    event_fastlane_service_type,        -- new field added 11.2.18_kvc
    event_fastlane_event_type,      -- new field added 11.2.18_kvc
    event_fastlane_record_id,           -- new field added 11.2.18_kvc
    event_fastlane_dc_id,           -- new field added 11.2.18_kvc
    event_fastlane_region,          -- new field added 11.2.18_kvc              
    event_fastlane_resource_id,     -- new field added 11.2.18_kvc
    event_fastlane_resource_name,       -- new field added 11.2.18_kvc
    event_fastlane_attr1,           -- new field added 11.2.18_kvc
    event_fastlane_attr2,           -- new field added 11.2.18_kvc
    event_fastlane_attr3,           -- new field added 11.2.18_kvc
    fastlane_impact_category,           -- new field added 11.2.18_kvc 
    fastlane_impact_value,          -- new field added 11.2.18_kvc 
    event_earned_start_dtt,         --new field added 05.8.19_kvc 
    event_earned_end_dtt,           --new field added 05.8.19_kvc
    event_fastlane_is_backbill,     --new field added 05.8.19_kvc
    fastlane_inv_deal_code,         --new field added 05.8.19_kvc
    fastlane_inv_grp_code,          --new field added 05.8.19_kvc
    fastlane_inv_sub_grp_code,      --new field added 05.8.19_kvc
    fastlane_inv_is_backbill,            --new field added 05.8.19_kvc
	ACTIVITY_ATTR4,  -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR5,  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR6,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Tax_Element_Id,     --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
from
    brm_dedicated_billing2
where
     master_unique_id not in  (select master_unique_id from  raw_brm_dedicated_billing_daily_master_audit));

--********************************************************************************************************************* 

CREATE OR REPLACE TEMP TABLE brm_dedicated_billing_cleanup AS (
select distinct
   cast(CONCAT(COALESCE(cast(COALESCE(b.event_poid_id0,item_poid_id0) as string), 'X') ,'--', 
    COALESCE(cast(ebi_rec_id as string), 'X')) as string)            as master_unique_id,
    COALESCE(da.account_obj_id0,da2.account_poid)                        as account_poid,
    COALESCE(da.parent_account_poid,da2.parent_account_poid)                 as parent_account_poid,
    COALESCE(da.child_account_no,da2.child_account_no)                   as child_account_no,
    COALESCE(da.brm_account_no,da2.brm_account_no)                       as brm_account_no,
    COALESCE(da.profile_poid,da2.profile_poid)                           as profile_poid,
    COALESCE(da.profile_type,da2.profile_type)                           as profile_type,
    COALESCE(da.account_obj_id0, da2.account_poid)                       as account_obj_id0,
    COALESCE(da.account_number,da2.account_number)                       as account_number,
    COALESCE(da.gl_segment,da2.gl_segment)                           as gl_segment,
    COALESCE(da.customer_type,da2.customer_type)                         as customer_type,
    COALESCE(da.customer_type_descr,da2.customer_type_descr)                 as customer_type_descr,
    COALESCE(da.billing_segment,da2.billing_segment)                     as billing_segment,
    COALESCE(da.billing_seg_descr,da2.billing_seg_descr)                     as billing_seg_descr,
    COALESCE(da.contracting_entity,da2.contracting_entity)               as contracting_entity,
    COALESCE(da.support_team,da2.support_team)                           as support_team,
    COALESCE(da.support_team_descr,da2.support_team_descr)               as support_team_descr,
    COALESCE(da.team_customer_type,da2.team_customer_type)               as team_customer_type,
    COALESCE(da.team_cust_class,da2.team_cust_class)                     as team_cust_class,
    COALESCE(da.org_value,da2.org_value)                                 as org_value,
    COALESCE(da.organization,da2.organization)                           as organization,
    COALESCE(da.business_unit,da2.business_unit)                         as business_unit,
    COALESCE(da.payment_term,da2.payment_term)                           as payment_term,
    COALESCE(da.business_type,da2.business_type)                         as business_type,
    COALESCE(da.business_type_descr,da2.business_type_descr)                 as business_type_descr,
    bill_no,
    0                                                        as rax_bill_poid,
    bill_poid_id0                                                as bill_poid,
    current_total,
    total_due,
    bill_start_date,
    bill_end_date,
    bill_mod_date,
    item_poid_id0,
    item_no,
    item_effective_date,
    item_mod_date,
    item_name,
    item_status,
    item_bill_obj_id0,
    service_obj_type,
    item_type,
    b.event_poid_id0,
    event_item_obj_id0,
    event_type,
    event_create_dtt,
    event_mod_dtt,
    event_start_dtt,
    event_end_dtt,
    event_descr,
    event_name,
    event_program_name,
    event_rum_name,
    event_sys_descr,
    event_usage_type,
    event_invoice_data,
    event_service_obj_id0,
    event_service_obj_type,
    event_session_obj_ido,
    event_rerate_obj_id0,
    bil_cyc_resource_id,
    bil_cyc_quantity,
    service_login_site_id,
    service_obj_id0,
    service_name,
    service_type_classid,
    service_acct_poid,
    service_balgrp_poid,
    service_status,
    service_eff_date,
    ded_obj_id0,
    ded_quantity,
    ded_orig_quantity,
    ded_rax_uom,
    ded_period_name,
    ded_inv_date,
    ded_usage_type,
    ded_billing_type,
    ded_device_id,
    ded_device_name,
    ded_bill_start,
    ded_bill_end,
    ded_prepay_start,
    ded_prepay_end,
    ded_batch_id,
    ded_login_siteid,
    ded_product_name,
    ded_prod_type,
    ded_event_desc,
    ded_make_model,
    ded_os,
    ded_ram,
    ded_processor,
    ded_record_id,
    ded_data_center_id,
    ded_region,
    ded_currency_id,
    ebi_obj_id0,
    ebi_rec_id,
    ebi_amount                      as ebi_amount,
    ebi_amount_orig,
    ebi_gl_id,
    ebi_impact_category,
    ebi_impact_type,
    ebi_product_obj_id0,
    ebi_product_obj_type,
    ebi_quantity,
    ebi_rate_tag,
    ebi_resource_id                 as ebi_currency_id,
    ebi_tax_code,
    ebi_rum_id,
    ebi_discount,
    ebi_offering_obj_id0,
    ebi_offering_obj_type,
    ebi_bal_grp_obj_ido,
    ebi_bal_grp_obj_type,
    misc_event_billing_type,            ---credit_type
    misc_event_billing_type_reason_id,  --credit_reason_id
    misc_event_billing_type_record_id,
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
    COALESCE(da.line_of_business,da2.line_of_business)               as line_of_business,
    activity_service_type               as event_fastlane_service_type,
    activity_event_type                 as event_fastlane_event_type,
    activity_record_id                  as event_fastlane_record_id,
    activity_dc_id                      as event_fastlane_dc_id,
    activity_region                      as event_fastlane_region,
    activity_resource_id                     as event_fastlane_resource_id,
    activity_resource_name              as event_fastlane_resource_name,
    activity_attr1                      as event_fastlane_attr1,
    activity_attr2                      as event_fastlane_attr2,
    activity_attr3                      as event_fastlane_attr3,
    fastlane_impact_category,           -- new field added 11.2.18_kvc 
    fastlane_impact_value,          -- new field added 11.2.18_kvc 
    event_earned_start_dtt,         --new field added 05.8.19_kvc 
    event_earned_end_dtt,           --new field added 05.8.19_kvc
    ACTIVITY_backbill_flag                     as event_fastlane_is_backbill, --new field added 05.8.19_kvc
    fastlane_inv_deal_code,         --new field added 05.8.19_kvc
    fastlane_inv_grp_code,          --new field added 05.8.19_kvc
    fastlane_inv_sub_grp_code,      --new field added 05.8.19_kvc
    fastlane_inv_is_backbill,            --new field added 05.8.19_kvc
	ACTIVITY_ATTR4,  -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR5,  --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR6,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Tax_Element_Id,     --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	A.Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
from
  stage_one.raw_dedicated_item_audit_step1 a-- dbo.stage_dedicated_event_load_step2 a'
inner join
    stage_one.raw_dedicated_event_audit_step3 b
on a.item_poid_id0=b.event_item_obj_id0
left outer join
    stage_one.raw_dedicated_adj_audit_step2 cc
on b.event_poid_id0=cc.event_poid_id0  
left outer join
    stage_one.raw_dedicated_rax_events_audit_load_step4  c 
on b.event_poid_id0=c.ded_obj_id0
left outer join
   stage_one.raw_dedicated_impacts_audit_load_step6 d  
on b.event_poid_id0=d.ebi_obj_id0
left outer join 
    stage_one.raw_dedicated_audit_fast_lane fl 
on b.event_poid_id0=fl.fastlane_event_poid_id0
left outer join 
    raw_brm_dedicated_invoice_aggregate_total_audit e 
on a.item_bill_obj_id0=e.bill_poid_id0
left outer join 
    stage_one.raw_brm_dedicated_account_profile da 
on da.account_poid=event_account_poid
left outer join 
    stage_one.raw_brm_dedicated_account_profile da2 
on da2.account_poid=a.account_obj_id0
where
    COALESCE(ebi_amount,0) = 0
and e.bill_no not in  (select bill_no from  raw_brm_dedicated_billing_daily_master_audit)
);

/*--[Ashok: Commented below CREATE INDEX]
 --********************************************************************************************************************* 
create index ix_master_unique_id on #BRM_Dedicated_Billing_Cleanup (master_unique_id)
--*********************************************************************************************************************  
*/
update
    brm_dedicated_billing_cleanup
set
    ebi_amount=current_total
WHERE true;

--*********************************************************************************************************************  

CREATE Or REPLACE TEMP TABLE clenaupdupes AS (

   select
 * 
from  
    brm_dedicated_billing_cleanup  
where  bill_no in (select
       bill_no
    from
    (
    select 
       bill_no, count(master_unique_id) as count_master_unique_id
    from
       brm_dedicated_billing_cleanup
    group by
       bill_no
    having
       count(bill_no) >1
       )a
       )
);

--********************************************************************************************************************* 
DELETE FROM brm_dedicated_billing_cleanup  where bill_no in  (select bill_no from  clenaupdupes);
--********************************************************************************************************************* 



INSERT INTO brm_dedicated_billing_cleanup  (
select 
    a.*
from 
    clenaupdupes a
inner join
(
select 
    max(event_poid_id0) as max_event_poid_id0, bill_no
from
     clenaupdupes 
group by  
    bill_no
)b
on a.bill_no=b.bill_no
and a.event_poid_id0=b.max_event_poid_id0   );
--*********************************************************************************************************************  
insert into
    raw_brm_dedicated_billing_daily_master_audit
select 
    *
from
    brm_dedicated_billing_cleanup
where
     master_unique_id not in  (select master_unique_id from  raw_brm_dedicated_billing_daily_master_audit)  ;

--*********************************************************************************************************************
--select * from brm_dedicated_billing_daily_stage_master where bill_poid is null
update 
    raw_brm_dedicated_billing_daily_master_audit
set
   gl_segment='.dedicated'
where
    gl_segment <> '.dedicated';
  
--*********************************************************************************************************************

CREATE OR REPLACE TEMP TABLE dupes AS (
  select
 * 
from  
    raw_brm_dedicated_billing_daily_master_audit 
where  master_unique_id in (select
       master_unique_id
    from
    (
    select master_unique_id, count(master_unique_id) as count_master_unique_id
    from
       raw_brm_dedicated_billing_daily_master_audit
    group by
       master_unique_id
    having
       count(master_unique_id) >1
       )a
       )  
);

--********************************************************************************************************************* 
delete from  raw_brm_dedicated_billing_daily_master_audit  where master_unique_id in  (select master_unique_id from  dupes);
--SELECT DISTINCT BILL_NO FROM  Net_Revenue.[dbo].[BRM_Dedicated_Invoice_line_Item_Audit] WHERE  BILL_NO NOT IN (SELECT DISTINCT BILL_NO FROM  #BRM_Dedicated_Billing_Daily_Stage_Master_Audit )
--********************************************************************************************************************* 

delete from  stage_one.raw_brm_dedicated_billing_daily_master_audit where 1=1;



insert into
    stage_one.raw_brm_dedicated_billing_daily_master_audit (
        master_unique_id,
account_poid,
parent_account_poid,
child_account_no,
brm_account_no,
profile_poid,
profile_type,
account_obj_id0,
account_number,
gl_segment,
customer_type,
customer_type_descr,
billing_segment,
billing_seg_descr,
contracting_entity,
support_team,
support_team_descr,
team_customer_type,
team_cust_class,
org_value,
organization,
business_unit,
payment_term,
business_type,
business_type_descr,
bill_no,
rax_bill_poid,
bill_poid,
current_total,
total_due,
bill_start_date,
bill_end_date,
bill_mod_date,
item_poid_id0,
item_no,
item_effective_date,
item_mod_date,
item_name,
item_status,
service_obj_type,
item_type,
item_bill_obj_id0,
event_poid_id0,
event_item_obj_id0,
event_type,
event_create_dtt,
event_mod_dtt,
event_start_dtt,
event_end_dtt,
event_descr,
event_name,
event_program_name,
event_rum_name,
event_sys_descr,
event_usage_type,
event_invoice_data,
event_service_obj_id0,
event_service_obj_type,
event_session_obj_ido,
event_rerate_obj_id0,
bil_cyc_resource_id,
bil_cyc_quantity,
service_login_site_id,
service_obj_id0,
service_name,
service_type_classid,
service_acct_poid,
service_balgrp_poid,
service_status,
service_eff_date,
ded_obj_id0,
ded_quantity,
ded_orig_quantity,
ded_rax_uom,
ded_period_name,
ded_inv_date,
ded_usage_type,
ded_billing_type,
ded_device_id,
ded_device_name,
ded_bill_start,
ded_bill_end,
ded_prepay_start,
ded_prepay_end,
ded_batch_id,
ded_login_siteid,
ded_product_name,
ded_prod_type,
ded_event_desc,
ded_make_model,
ded_os,
ded_ram,
ded_processor,
ded_record_id,
ded_data_center_id,
ded_region,
ded_currency_id,
ebi_obj_id0,
ebi_rec_id,
ebi_amount,
ebi_amount_orig,
ebi_gl_id,
ebi_impact_category,
ebi_impact_type,
ebi_product_obj_id0,
ebi_product_obj_type,
ebi_quantity,
ebi_rate_tag,
ebi_currency_id,
ebi_tax_code,
ebi_rum_id,
ebi_discount,
ebi_offering_obj_id0,
ebi_offering_obj_type,
ebi_bal_grp_obj_ido,
ebi_bal_grp_obj_type,
misc_event_billing_type,
misc_event_billing_type_reason_id,
misc_event_billing_type_record_id,
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
master_tbl_loaddtt,
line_of_business,
event_fastlane_service_type,
event_fastlane_event_type,
event_fastlane_record_id,
event_fastlane_dc_id,
event_fastlane_region,
event_fastlane_resource_id,
event_fastlane_resource_name,
event_fastlane_attr1,
event_fastlane_attr2,
event_fastlane_attr3,
fastlane_impact_category,
fastlane_impact_value,
event_earned_start_dtt,
event_earned_end_dtt,
event_fastlane_is_backbill,
fastlane_inv_deal_code,
fastlane_inv_grp_code,
fastlane_inv_sub_grp_code,
fastlane_inv_is_backbill )
select
    master_unique_id,
    account_poid,
    parent_account_poid,
    child_account_no,
    brm_account_no,
    profile_poid,
    profile_type,
    account_obj_id0,
    account_number,
    gl_segment,
    customer_type,
    customer_type_descr,
    billing_segment,
    billing_seg_descr,
    contracting_entity,
    support_team,
    support_team_descr,
    team_customer_type,
    team_cust_class,
    org_value,
    organization,
    business_unit,
    payment_term,
    business_type,
    business_type_descr,
    bill_no,
    rax_bill_poid,
    bill_poid,
    current_total,
    total_due,
    cast(bill_start_date as Date) as bill_start_date,
    cast(bill_end_date as Date) as bill_end_date,
    cast(bill_mod_date as Date) as bill_mod_date,
    item_poid_id0,
    item_no,
    item_effective_date,
    item_mod_date,
    item_name,
    item_status,
    service_obj_type,
    item_type,
    item_bill_obj_id0,
    event_poid_id0,
    event_item_obj_id0,
    event_type,
    event_create_dtt,
    event_mod_dtt,
    event_start_dtt,
    event_end_dtt,
    event_descr,
    event_name,
    event_program_name,
    event_rum_name,
    event_sys_descr,
    event_usage_type,
    event_invoice_data,
    event_service_obj_id0,
    event_service_obj_type,
    event_session_obj_ido,
    event_rerate_obj_id0,
    bil_cyc_resource_id,
    bil_cyc_quantity,
    service_login_site_id,
    service_obj_id0,
    service_name,
    service_type_classid,
    service_acct_poid,
    service_balgrp_poid,
    service_status,
    service_eff_date,
    ded_obj_id0,
    ded_quantity,
    ded_orig_quantity,
    ded_rax_uom,
    ded_period_name,
    ded_inv_date,
    ded_usage_type,
    ded_billing_type,
    ded_device_id,
    ded_device_name,
    ded_bill_start,
    ded_bill_end,
    ded_prepay_start,
    ded_prepay_end,
    ded_batch_id,
    ded_login_siteid,
    ded_product_name,
    ded_prod_type,
    ded_event_desc,
    ded_make_model,
    ded_os,
    ded_ram,
    ded_processor,
    ded_record_id,
    ded_data_center_id,
    ded_region,
    ded_currency_id,
    ebi_obj_id0,
    ebi_rec_id,
    ebi_amount,
    ebi_amount_orig,
    ebi_gl_id,
    ebi_impact_category,
    ebi_impact_type,
    ebi_product_obj_id0,
    ebi_product_obj_type,
    ebi_quantity,
    ebi_rate_tag,
    ebi_currency_id,
    ebi_tax_code,
    ebi_rum_id,
    ebi_discount,
    ebi_offering_obj_id0,
    ebi_offering_obj_type,
    ebi_bal_grp_obj_ido,
    ebi_bal_grp_obj_type,
    misc_event_billing_type,            ---credit_type
    misc_event_billing_type_reason_id,  --credit_reason_id
    misc_event_billing_type_record_id,
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
    CURRENT_DATETIME()                           as master_tbl_loaddtt,
    line_of_business,
    event_fastlane_service_type,    -- new field added 11.2.18_kvc
    event_fastlane_event_type,      -- new field added 11.2.18_kvc
    event_fastlane_record_id,       -- new field added 11.2.18_kvc
    event_fastlane_dc_id,           -- new field added 11.2.18_kvc
    event_fastlane_region,          -- new field added 11.2.18_kvc              
    event_fastlane_resource_id,     -- new field added 11.2.18_kvc
    event_fastlane_resource_name,   -- new field added 11.2.18_kvc
    event_fastlane_attr1,           -- new field added 11.2.18_kvc
    event_fastlane_attr2,           -- new field added 11.2.18_kvc
    event_fastlane_attr3,           -- new field added 11.2.18_kvc
    fastlane_impact_category,       -- new field added 11.2.18_kvc  
    fastlane_impact_value,          -- new field added 11.2.18_kvc 
    event_earned_start_dtt,         --new field added 05.8.19_kvc 
    event_earned_end_dtt,           --new field added 05.8.19_kvc
    cast(event_fastlane_is_backbill as int64) as event_fastlane_is_backbill,     --new field added 05.8.19_kvc
    fastlane_inv_deal_code,         --new field added 05.8.19_kvc 
    fastlane_inv_grp_code,          --new field added 05.8.19_kvc
    fastlane_inv_sub_grp_code,      --new field added 05.8.19_kvc
    cast(fastlane_inv_is_backbill as int64) as  fastlane_inv_is_backbill           --new field added 05.8.19_kvc
from
      raw_brm_dedicated_billing_daily_master_audit;


drop table if exists raw_brm_dedicated_invoice_aggregate_total_audit;
drop table clenaupdupes;
drop table dupes;
drop table brm_dedicated_billing2;
drop table brm_dedicated_billing_cleanup;
drop table raw_brm_dedicated_billing_daily_master_audit;
END;
