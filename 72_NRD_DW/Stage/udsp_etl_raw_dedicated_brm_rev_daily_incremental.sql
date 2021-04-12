CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_dedicated_brm_rev_daily_incremental`()

BEGIN  
/*  
  
Modification log  
 1) Copied SP from 72 server  
 2) new field added 7.10.2019 anil2912 as per uday request for NRD Staging  
 3) Added new fields (Tax_Type_Id and Tax_Element_Id) 7.23.2019 rahu4260 as per uday request for NRD staging  
 4) Added new field bill_created_date on 7.31.2019 rahu4260 as per uday request for NRD staging 
 5) Commented ITem_Total filter by anil5217 
 6) Added adjustment filter
*/  
--------------------------------------------------------------------------------------------------------------------        
DECLARE RunTime  INT64;        
DECLARE StartTime datetime DEFAULT CURRENT_DATETIME()  ;      
DECLARE jobdate datetime;
DECLARE V_GETDATE DATETIME;
DECLARE MAXDATE datetime;
DECLARE GETDATE_UNIX  INT64;
DECLARE TSQL string;
DECLARE GETDATEMAX1_UNIX INT64;  
--------------------------------------------------------------------------------------------------------------------    
SET MAXDATE =(SELECT MAX(Event_Mod_Dtt) FROM stage_two_dw.stage_dedicated_inv_event_detail) ;
SET jobdate= DATETIME_TRUNC(DATE_SUB(MAXDATE, INTERVAL -1 DAY), DAY);--cast(convert(varchar,MAXDATE-1,101)as datetime)
SET V_GETDATE = jobdate;        
SET GETDATE_UNIX = UNIX_SECONDS(CAST(V_GETDATE AS  TIMESTAMP)) ;  
SET GETDATEMAX1_UNIX = UNIX_SECONDS(TIMESTAMP "2017-09-01 00:00:00.000");  



CREATE OR REPLACE TABLE stage_one.raw_dedicated_item_load_step   AS 
SELECT DISTINCT   
    ACCOUNT_POID_ID0,         
    GL_SEGMENT,         
    POID_ID0          AS ITEM_POID_ID0,    
    Bill_Obj_Id0         AS ITEM_Bill_Obj_Id0,  
    item_no          AS ITEM_NO,  
   -- CAST(dateadd(ss,effective_t, '1970-01-01') as date) 
    cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(effective_t  as int64) second)  as datetime)	 AS ITEM_EFFECTIVE_DATE ,
	    --CAST(dateadd(ss,mod_t, '1970-01-01') as date)  AS ITEM_MOD_DATE , 
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(mod_t  as int64) second)  as datetime)	AS ITEM_MOD_DATE,
    NAME           AS ITEM_NAME,    
    STATUS          AS ITEM_STATUS,        
    SERVICE_OBJ_TYPE        AS SERVICE_OBJ_TYPE,        
    POID_TYPE          AS ITEM_TYPE,   
    Item_gl_segment         AS ITEM_GL_SEGMENT,  
    ITEM_TOTAL,    
    CURRENT_DATETIME()          AS Tbl_Load_Date  
FROM (
SELECT DISTINCT   
    acct.poid_id0     AS ACCOUNT_POID_ID0,         
    acct.gl_segment     AS GL_SEGMENT,         
    i.POID_ID0,    
    I.Bill_Obj_Id0,  
    I.AR_BILL_OBJ_ID0,  
    i.item_no,  
    i.effective_t,       
    i.mod_t,        
    i.NAME,    
    I.ITEM_TOTAL,      
    i.STATUS,        
    i.SERVICE_OBJ_TYPE,     
    i.gl_segment     AS Item_gl_segment,     
    i.POID_TYPE  
FROM  
    `rax-landing-qa`.brm_ods.item_t  i
INNER JOIN  
     `rax-landing-qa`.brm_ods.account_t acct   
ON I.ACCOUNT_OBJ_ID0= acct.Poid_Id0  
WHERE    1=1
 and   I.mod_t >= GETDATE_UNIX --CAST(GETDATE_UNIX AS STRING)
--AND ITEM_TOTAL<>0    --anil5217 made changes based on DATA-5929
AND (IfNULL(I.Bill_Obj_Id0,0)+IfNULL(I.AR_BILL_OBJ_ID0,0)) <>0  
and account_no like '030%' 
);   
--*********************************************************************************************************************   


---*********************************************************************************************************************   
CREATE OR REPLACE TABLE stage_one.raw_dedicated_invoice_load_step   as 
SELECT  
    ACCOUNT_POID_ID0,         
    GL_SEGMENT,         
    ITEM_POID_ID0,    
    ITEM_Bill_Obj_Id0,  
    ITEM_NO,  
    ITEM_EFFECTIVE_DATE ,        
    ITEM_MOD_DATE,        
    ITEM_NAME,     
    ITEM_STATUS,        
    SERVICE_OBJ_TYPE,        
    ITEM_TYPE,   
    ITEM_GL_SEGMENT,  
    BILL_POID_ID0,  
    BILL_NO,  
    cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(BILL_START_DATE  as int64) second)  as datetime)	 AS BILL_START_DATE,
   cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(BILL_MOD_DATE  as int64) second)  as datetime) AS BILL_MOD_DATE ,   
  cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(BILL_END_DATE  as int64) second)  as datetime)  AS BILL_END_DATE ,         
    POID_TYPE,  
    ITEM_TOTAL,  
    CURRENT_TOTAL,  
    TOTAL_DUE,    
    Tbl_Load_Date,  
 Bill_Created_Date  
FROM  
   ( 
SELECT  
    ACCOUNT_POID_ID0,         
    GL_SEGMENT,         
    ITEM_POID_ID0,    
    ITEM_Bill_Obj_Id0,  
    ITEM_NO,  
    ITEM_EFFECTIVE_DATE ,        
    ITEM_MOD_DATE ,        
    ITEM_NAME,     
    ITEM_STATUS,        
    SERVICE_OBJ_TYPE,        
    ITEM_TYPE,   
    ITEM_GL_SEGMENT,  
    Bill.poid_id0     AS BILL_POID_ID0,  
    BILL_NO,  
    Bill.start_t     AS BILL_START_DATE,  
    Bill.mod_t      AS BILL_MOD_DATE,  
    Bill.end_t      AS BILL_END_DATE,   
    Bill.POID_TYPE     AS POID_TYPE,  
    IFNULL(CURRENT_TOTAL,0)   AS CURRENT_TOTAL,  
    IFNULL(TOTAL_DUE,0)    AS TOTAL_DUE,  
    IFNULL(ITEM_TOTAL,0)    AS ITEM_TOTAL,  
    CURRENT_DATETIME()      AS Tbl_Load_Date,  
 Bill.Created_T            AS Bill_Created_Date --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging   
FROM  
    stage_one.raw_dedicated_item_load_step  A  
INNER JOIN  
     `rax-landing-qa`.brm_ods.bill_t  Bill  
ON  A.ITEM_Bill_Obj_Id0=Bill.poid_id0  
WHERE  
    ITEM_STATUS<> 1  
  
)  ;


 
---*********************************************************************************************************************   
CREATE OR REPLACE TABLE stage_one.raw_dedicated_event_list_step1   AS 
SELECT *  
FROM ( 
SELECT  
   e.POID_DB  
      ,e.POID_ID0  
      ,e.POID_TYPE  
      ,e.POID_REV  
      ,e.CREATED_T  
      ,e.MOD_T  
      ,e.READ_ACCESS  
      ,e.WRITE_ACCESS  
      ,e.ACCOUNT_OBJ_DB  
      ,e.ACCOUNT_OBJ_ID0  
      ,e.ACCOUNT_OBJ_TYPE  
      ,e.ACCOUNT_OBJ_REV  
      ,e.ARCHIVE_STATUS  
      ,e.CURRENCY  
      ,e.DESCR  
      ,e.EFFECTIVE_T  
      ,e.END_T  
      ,e.EARNED_START_T  
      ,e.EARNED_END_T  
      ,e.EARNED_TYPE  
      ,e.EVENT_NO  
      ,e.FLAGS  
      ,e.GROUP_OBJ_DB  
      ,e.GROUP_OBJ_ID0  
      ,e.GROUP_OBJ_TYPE  
      ,e.GROUP_OBJ_REV  
      ,e.ITEM_OBJ_DB  
      ,e.ITEM_OBJ_ID0  
      ,e.ITEM_OBJ_TYPE  
      ,e.ITEM_OBJ_REV  
      ,e.NAME  
      ,e.PROGRAM_NAME  
      ,e.PROVIDER_DESCR  
      ,e.PROVIDER_ID_DB  
      ,e.PROVIDER_ID_ID0  
      ,e.PROVIDER_ID_TYPE  
      ,e.PROVIDER_ID_REV  
      ,e.PROVIDER_IPADDR  
      ,e.RUM_NAME  
      ,e.UNIT  
      ,e.TOD_MODE  
      ,e.TIMEZONE_MODE  
      ,e.TIMEZONE_ID  
      ,e.RATED_TIMEZONE_ID  
      ,e.TIMEZONE_ADJ_START_T  
      ,e.TIMEZONE_ADJ_END_T  
      ,e.MIN_QUANTITY  
      ,e.MIN_UNIT  
      ,e.INCR_QUANTITY  
      ,e.INCR_UNIT  
      ,e.ROUNDING_MODE  
      ,e.NET_QUANTITY  
      ,e.UNRATED_QUANTITY  
      ,e.SERVICE_OBJ_DB  
      ,e.SERVICE_OBJ_ID0  
      ,e.SERVICE_OBJ_TYPE  
      ,e.SERVICE_OBJ_REV  
      ,e.SESSION_OBJ_DB  
      ,e.SESSION_OBJ_ID0  
      ,e.SESSION_OBJ_TYPE  
      ,e.SESSION_OBJ_REV  
      ,e.RERATE_OBJ_DB  
      ,e.RERATE_OBJ_ID0  
      ,e.RERATE_OBJ_TYPE  
      ,e.RERATE_OBJ_REV  
      ,e.START_T  
      ,e.SYS_DESCR  
      ,e.TAX_LOCALES  
      ,e.TAX_SUPPLIER  
      ,e.USERID_DB  
      ,e.USERID_ID0  
      ,e.USERID_TYPE  
      ,e.USERID_REV  
      ,e.INVOICE_DATA  
      ,e.LOADER_BATCH_OBJ_DB  
      ,e.LOADER_BATCH_OBJ_ID0  
      ,e.LOADER_BATCH_OBJ_TYPE  
      ,e.LOADER_BATCH_OBJ_REV  
      ,e.BATCH_ID  
      ,e.ORIGINAL_BATCH_ID  
      ,e.USAGE_TYPE  
      ,e.PROFILE_LABEL_LIST  
      ,e.CLI_IPADDR  
      ,e.COMPACT_SUB_EVENT  
      ,e.dw_Timestamp  
   ,inv.Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging  
from  
    `rax-landing-qa`.brm_ods.event_t e    
INNER JOIN   
    stage_one.raw_dedicated_invoice_load_step inv     
ON E.Item_Obj_Id0=inv.ITEM_POID_ID0     
inner join  
    `rax-landing-qa`.brm_ods.account_t a    
on e.account_obj_id0 = a.poid_id0  
WHERE  
 account_no like '030%'  
AND (e.batch_id IS NULL  OR LOWER(e.batch_Id) not like 'rerating%')  
AND e.rerate_obj_id0 = 0  
AND (
	LOWER(e.poid_type) like '/event/activity/rax/dedicated%'  
OR  LOWER(e.poid_type) like '/event/delayed/rax/dedicated%'  
OR  LOWER(e.poid_type) like '%adjustment%'
OR  LOWER(e.poid_type) ='/event/delayed/rax/dedicated/infrastructure/uptime'  
OR  LOWER(e.poid_type) ='/event/delayed/rax/dedicated/infrastructure/services'  
OR  LOWER(e.poid_type) = '/event/billing/cycle/tax'  
OR  LOWER(e.poid_type) = '/event/billing/cycle/discount'  
OR  LOWER(e.poid_type) = '/event/billing/cycle/fold'  
OR  LOWER(e.poid_type) = '/event/billing/product/fee/purchase'  
OR  LOWER(e.poid_type) = '/event/activity/rax/fastlane'  
OR  LOWER(e.poid_type) like '/event/billing/product/fee/cycle/cycle%'  
OR  LOWER(e.SERVICE_OBJ_TYPE) like  '%/rax/fastlane/%'  
OR  LOWER(e.SERVICE_OBJ_TYPE) like '%datapipe%' 
)  
) ; 

CREATE OR REPLACE TABLE stage_one.raw_dedicated_fast_lane  as 
SELECT  
    fastlane_EVENT_POID_ID0,  
    INV_GRP_CODE          AS ACTIVITY_SERVICE_TYPE,   --new ield added 1.20.2017 kvc  
    INV_SUB_GRP_CODE         AS ACTIVITY_EVENT_TYPE,  --new ield added 1.20.2017 kvc  
    RECORD_ID           AS ACTIVITY_RECORD_ID,  --new ield added 1.20.2017 kvc  
    DATA_CENTER_ID          AS ACTIVITY_DC_ID,   --new ield added 1.20.2017 kvc  
    REGION           AS ACTIVITY_REGION,   --new ield added 1.20.2017 kvc  
    resource_id          AS ACTIVITY_RESOURCE_ID, --new ield added 1.20.2017 kvc  
    resource_name          AS ACTIVITY_RESOURCE_NAME,  --new ield added 1.20.2017 kvc  
    attr1            AS ACTIVITY_ATTR1,   --new ield added 1.20.2017 kvc  
    attr2            AS ACTIVITY_ATTR2,   --new ield added 1.20.2017 kvc  
    attr3            AS ACTIVITY_ATTR3,   --new ield added 1.20.2017 kvc  
    CURRENT_DATETIME()           AS Tbl_Load_Date,  
    ACTIVITY_IS_BACKBILL,  
 Attr4 AS ACTIVITY_ATTR4,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging  
 Attr5 AS ACTIVITY_ATTR5,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging  
 Attr6 AS ACTIVITY_ATTR6 --,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging  
  
FROM  
    (  
SELECT DISTINCT  
    E.POID_ID0      AS fastlane_EVENT_POID_ID0,  
    fastlane.INV_GRP_CODE,   --new ield added 1.20.2017 kvc  
    fastlane.INV_SUB_GRP_CODE,  --new ield added 1.20.2017 kvc  
    fastlane.RECORD_ID,    --new ield added 1.20.2017 kvc  
    fastlane.DATA_CENTER_ID,   --new ield added 1.20.2017 kvc  
    fastlane.REGION,    --new ield added 1.20.2017 kvc  
    fastlane.resource_id,   --new ield added 1.20.2017 kvc  
    fastlane.resource_name,   --new ield added 1.20.2017 kvc  
    fastlane.attr1,     --new ield added 1.20.2017 kvc  
    fastlane.attr2,     --new ield added 1.20.2017 kvc  
    fastlane.attr3,     --new ield added 1.20.2017 kvc  
    0       AS ACTIVITY_IS_BACKBILL,  --new field added 4/30/2019  
 fastlane.Attr4,      
 fastlane.Attr5,      
 fastlane.Attr6      
FROM  
   stage_one.raw_dedicated_event_list_step1 e     
INNER JOIN    
    `rax-landing-qa`.brm_ods.event_act_rax_fastlane_t AS fastlane
ON E.poid_id0 = fastlane.obj_id0   
);

 
CREATE OR REPLACE TABLE stage_one.raw_dedicated_event_load_step2 AS
SELECT  
    poid_id0          AS EVENT_POID_ID0,  
    EVENT_account_poid,  
    Item_Obj_Id0         AS EVENT_Item_Obj_Id0,  
    poid_type          AS EVENT_type,  
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(created_t  as int64) second)  as datetime) AS EVENT_create_dtt, 
    cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(mod_t  as int64) second)  as datetime)  AS EVENT_mod_dtt,  
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(start_t  as int64) second)  as datetime) AS EVENT_start_dtt,  
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(end_t  as int64) second)  as datetime) AS EVENT_end_dtt,  
    descr           AS EVENT_descr,  
    EVENT_name,  
    program_name         AS EVENT_program_name,  
    rum_name          AS EVENT_rum_name,  
    sys_descr          AS EVENT_sys_descr,  
    usage_type          AS EVENT_usage_type,  
    invoice_data         AS EVENT_invoice_data,  
    EVENT_service_obj_id0,  
    service_obj_type        AS EVENT_service_obj_type,  
    SESSION_OBJ_ID0        AS EVENT_Session_obj_ido,  
    rerate_obj_id0         AS EVENT_rerate_obj_id0,  
    resource_id         AS BIL_CYC_resource_id,  
    quantity          AS BIL_CYC_quantity, --- no values  
    SERVICE_login_site_id,  
    LOGIN          AS SERVICE_obj_id0,  --aka site id/bill_site_id  
    SERVICE_name,  
    TYPE          AS service_type_classid,  
    SERVICE_Acct_poid,  
    BAL_GRP_OBJ_ID0        AS SERVICE_BalGrp_poid,  
    STATUS          AS SERVICE_Status,  
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(EFFECTIVE_T  as int64) second)  as datetime) AS SERVICE_eff_date,  
    CURRENT_DATETIME() AS Tbl_Load_Date,  
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(EARNED_START_T  as int64) second)  as datetime)  AS EVENT_earned_start_dtt,  
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(EARNED_END_T  as int64) second)  as datetime)   AS EVENT_earned_end_dtt,  
 Bill_Created_Date  
FROM (
SELECT    
    e.poid_id0,  
     e.ACCOUNT_OBJ_ID0    AS EVENT_account_poid,  
    e.Item_Obj_Id0,  
    e.poid_type,  
    e.created_t,  
    e.mod_t,  
    e.start_t,   
e.end_t,   
    e.EARNED_START_T,  
    e.EARNED_END_T,  
    e.descr,  
    e.name      AS EVENT_name,  
    e.program_name,  
    e.rum_name,  
    e.sys_descr,  
    e.usage_type,  
    e.invoice_data,  
    e.service_obj_id0    AS EVENT_service_obj_id0,  
    e.service_obj_type,  
    e.SESSION_OBJ_ID0,  
    e.rerate_obj_id0,  
    bil_cyc_fld.resource_id,  
    bil_cyc_fld.quantity,   
    service_obj_id0     AS SERVICE_login_site_id,  
    svc.LOGIN,    
    svc.NAME      AS SERVICE_name,  
    svc.TYPE,  
    svc.ACCOUNT_OBJ_ID0    AS SERVICE_Acct_poid,  
    svc.BAL_GRP_OBJ_ID0,  
    svc.STATUS,  
    svc.EFFECTIVE_T,  
 e.Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging  
FROM  
  stage_one.raw_dedicated_event_list_step1 e     
LEFT OUTER JOIN  
    `rax-landing-qa`.brm_ods.event_billing_cycle_fold_t AS  bil_cyc_fld   
ON e.poid_id0 = bil_cyc_fld.Obj_Id0   
LEFT OUTER JOIN   
    `rax-landing-qa`.brm_ods.service_t as svc 
on e.service_obj_id0 = svc.POID_ID0  
);



CREATE OR REPLACE TABLE stage_one.raw_dedicated_credit_event_load_step       AS
SELECT DISTINCT   
    EVENT_POID_ID0         AS MISC_EVENT_POID_ID0,    
    STRING          AS MISC_EVENT_BILLING_Type, ---Credit_Type  
    REASON_ID          AS MISC_EVENT_BILLING_Type_Reason_ID, --Credit_Reason_ID  
    rec_id          AS MISC_EVENT_BILLING_Type_record_id,  
    domain          AS string_domain,  
    version          AS string_version,       
    current_datetime()          AS Tbl_Load_Date  
FROM (
SELECT DISTINCT   
    e.EVENT_POID_ID0,  
    S.STRING,   
    EBM.REASON_ID,   
    ebm.rec_id,  
    s.domain,  
    s.version  
FROM  
  stage_one.raw_dedicated_event_load_step2 E    
INNER JOIN  
    `rax-landing-qa`.brm_ods.event_billing_misc_t EBM   
ON E.EVENT_POID_ID0 = EBM.OBJ_ID0   
INNER JOIN  
    `rax-landing-qa`.brm_ods.strings_t  S 
ON EBM.REASON_ID = S.STRING_ID   
AND EBM.REASON_DOMAIN_ID = S.VERSION  
and lower(s.domain) LIKE 'reason%'  
)  ;


CREATE OR REPLACE TABLE stage_one.raw_dedicated_rax_events_load_step4 as
SELECT DISTINCT   
    obj_id0         AS DED_obj_id0,  
    quantity         AS DED_quantity,  
    orig_Quantity        AS DED_orig_Quantity,  
    rax_uom         AS DED_rax_uom,   
    ATTR2          AS DED_period_name,  
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(inv_date_t  as int64) second)  as datetime) AS DED_inv_date,  
    usage_type         AS DED_usage_type,  
    billing_type        AS DED_billing_type,  
    device_id         AS DED_device_id,  
    device_name        AS DED_device_name,  
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(billing_start_t  as int64) second)  as datetime)  AS DED_bill_start,  
    cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(billing_end_t  as int64) second)  as datetime)   AS DED_bill_end,   
    cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(PREPAY_START_T  as int64) second)  as datetime)  AS DED_prepay_start,     
    cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(PREPAY_START_T  as int64) second)  as datetime) AS DED_prepay_end,    
    batch_id         AS DED_batch_id,  
    login         AS DED_login_siteid,  
    product_name        AS DED_product_name,  
    attr1          AS DED_prod_type,  
    ATTR3          AS DED_Event_Desc,  
    ATTR4          AS DED_Make_Model,  
    ATTR5          AS DED_OS,  
    ATTR6          AS DED_RAM,  
    ATTR7          AS DED_Processor,  
    record_id         AS DED_record_id,  
    data_center_id        AS DED_data_center_id,  
    region         AS DED_region,  
    resource_id        AS DED_currency_id,  
    current_datetime()          AS Tbl_Load_Date  
FROM( 
SELECT DISTINCT   
    eded.obj_id0,  
    eded.quantity,  
    eded.orig_Quantity,  
    eded.rax_uom,   
    ATTR2,  
    eded.inv_date_t,  
    eded.usage_type,  
    eded.billing_type,  
    eded.device_id,  
    eded.device_name,  
    eded.billing_start_t,  
    eded.billing_end_t,    
    eded.PREPAY_START_T,      
    eded.PREPAY_END_T,    
    eded.batch_id,  
    eded.login,  
    eded.product_name,  
    eded.attr1,  
    eded.ATTR3,  
    eded.ATTR4,  
    eded.ATTR5,  
    eded.ATTR6,  
    eded.ATTR7,  
    eded.record_id,  
    eded.data_center_id,  
    eded.region,  
    eded.resource_id  
from  
    stage_one.raw_dedicated_event_load_step2 A  
INNER JOIN  
    `rax-landing-qa`.brm_ods.event_act_rax_dedicated_t eded        
on  A.EVENT_POID_ID0 = eded.obj_id0  
)  ;

CREATE OR REPLACE TABLE stage_one.raw_dedicated_impacts_load_step5_initial as 
SELECT DISTINCT   
    EBI_OBJ_ID0,       
    EBI_Rec_id,  
    EBI_amount,  
    EBI_amount_Orig,  
    EBI_GL_ID,  
    EBI_CURRENCY_ID,  
    EBI_impact_category,  
    EBI_impact_type,  
    EBI_product_obj_id0,  
    EBI_product_obj_type,  
    EBI_quantity,  
    EBI_rate_tag,  
    EBI_resource_id,  
    EBI_tax_code,  
    EBI_rum_id,  
    EBI_discount,  
    EBI_offering_obj_id0,  
    EBI_offering_obj_type,  
    EBI_bal_grp_obj_ido,  
    EBI_bal_grp_obj_type,  
    Tbl_Load_Date,  
    FASTLANE_IMPACT_CATEGORY,  
    FASTLANE_IMPACT_VALUE,  
 Tax_Type_Id,  
 Tax_Element_Id,  
 Bill_Created_Date  
   
FROM (
SELECT    
    ebi.OBJ_ID0     AS EBI_OBJ_ID0,       
    ebi.rec_id      AS EBI_Rec_id,  
    ebi.amount      AS EBI_amount,  
    ebi.amount_orig     AS EBI_amount_Orig,  
    ebi.gl_id      AS EBI_GL_ID,  
    ebi.resource_ID     AS EBI_CURRENCY_ID,  
    ebi.impact_category    AS EBI_impact_category,  
    ebi.impact_type     AS EBI_impact_type,  
    ebi.product_obj_id0    AS EBI_product_obj_id0,  
    ebi.product_obj_type    AS EBI_product_obj_type,  
    ebi.quantity     AS EBI_quantity,  
    ebi.rate_tag     AS EBI_rate_tag,  
    ebi.resource_id     AS EBI_resource_id,  
    ebi.tax_code     AS EBI_tax_code,  
    ebi.rum_id      AS EBI_rum_id,  
    ebi.discount     AS EBI_discount,  
    ebi.offering_obj_id0    AS EBI_offering_obj_id0,  
    ebi.offering_obj_type   AS EBI_offering_obj_type,  
    ebi.BAL_GRP_OBJ_ID0   AS EBI_bal_grp_obj_ido,  
    ebi.BAL_GRP_OBJ_TYPE  AS EBI_bal_grp_obj_type,  
    current_datetime()      AS Tbl_Load_Date,  
    fastlane_invoicemap.IMPACT_Key   AS FASTLANE_IMPACT_CATEGORY, --new field added 11.2.2018 kvc  
    fastlane_invoicemap.IMPACT_VALUE  AS FASTLANE_IMPACT_VALUE, --new field added 11.2.2018 kvc  
 etj.Type        AS Tax_Type_Id,   --new field added 7.23.2019  
 etj.ELEMENT_ID       AS Tax_Element_Id, --new field added 7.23.2019  
 A.Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging  
   
FROM  
   stage_one.raw_dedicated_event_load_step2 A   
INNER JOIN   
  `rax-landing-qa`.brm_ods.event_bal_impacts_t  ebi  
ON  A.EVENT_POID_ID0=Ebi.Obj_Id0     
LEFT OUTER JOIN  
   `rax-landing-qa`.brm_ods.config_fastlane_invoice_map_t fastlane_invoicemap   
ON ebi.impact_category=fastlane_invoicemap.IMPACT_CODE  
LEFT OUTER JOIN      
    `rax-landing-qa`.brm_ods.event_tax_jurisdictions_t etj      
ON Ebi.OBJ_ID0= Etj.Obj_Id0            
AND EBI.rec_id= Etj.Element_Id    
WHERE  
    ebi.resource_id  < 999  
AND EVENT_rerate_obj_id0 = 0   
AND ifnull(EBI.AMOUNT,0) <> 0   
);


CREATE OR REPLACE TABLE stage_one.raw_dedicated_impacts_load_step5        as 
SELECT DISTINCT   
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
    EBI_resource_id,  
    EBI_tax_code,  
    EBI_rum_id,  
    EBI_discount,  
    EBI_offering_obj_id0,  
    EBI_offering_obj_type,  
    EBI_bal_grp_obj_ido,  
    EBI_bal_grp_obj_type,  
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
    current_datetime()      AS Tbl_Load_Date,  
    FASTLANE_IMPACT_CATEGORY,  
    FASTLANE_IMPACT_VALUE,  
    FASTLANE_INV_DEAL_CODE,  
    FASTLANE_INV_GRP_CODE,  
    FASTLANE_INV_SUB_GRP_CODE,  
    FASTLANE_INV_Is_Backbill,  
 Tax_Type_Id,  
 Tax_Element_Id,  
 Bill_Created_Date  
FROM (
SELECT    
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
  EBI_resource_id,  
    EBI_tax_code,  
    EBI_rum_id,  
    EBI_discount,  
    EBI_offering_obj_id0,  
    EBI_offering_obj_type,  
    EBI_bal_grp_obj_ido,  
    EBI_bal_grp_obj_type,  
    cur.currency         AS currency_abbrev,  
    cur.name          AS currency_name,  
    erm.rec_id          AS erm_rec_id,  
    erm.rum_name         AS erm_rum_name,   
    ifnull( prd.poid_id0, dsc.poid_id0)     AS product_poid,  
    ifnull(prd.poid_type, dsc.poid_type)    AS product_type,   
    ifnull( prd.descr, dsc.descr)      AS product_descr,    
    ifnull( prd.name, dsc.name)      AS product_name,   
    ifnull( prd.code, dsc.code)      AS product_code,   
    ifnull( prd.permitted, dsc.permitted)    AS prod_permitted,  
    ifnull( prd.type, dsc.type)     AS product_type2,  
    FASTLANE_IMPACT_CATEGORY,  
    FASTLANE_IMPACT_VALUE,  
    DEAL_CODE          AS FASTLANE_INV_DEAL_CODE,     --new ield added 5.1.2019 kvc       
    INV_GRP_CODE         AS FASTLANE_INV_GRP_CODE,     --new ield added 5.1.2019 kvc       
    INV_SUB_GRP_CODE        AS FASTLANE_INV_SUB_GRP_CODE,   --new ield added 5.1.2019 kvc       
    0               AS FASTLANE_INV_Is_Backbill,     --new ield added 5.1.2019 kvc  
 Tax_Type_Id,                --new field added 7.23.2019   
 Tax_Element_Id,                --new field added 7.23.2019   
 A.Bill_Created_Date              --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging        
FROM  
    stage_one.raw_dedicated_impacts_load_step5_initial A   
LEFT OUTER JOIN  
  `rax-landing-qa`.brm_ods.ifw_currency  CUR 
ON EBI_resource_id = CUR.CURRENCY_ID  
LEFT OUTER JOIN  
    `rax-landing-qa`.brm_ods.event_rum_map_t  AS erm  
on A.EBI_OBJ_ID0= Erm.Obj_Id0    
LEFT OUTER JOIN  
    `rax-landing-qa`.brm_ods.product_t prd   
on EBI_product_obj_id0 = prd.poid_id0  
LEFT OUTER JOIN  
   `rax-landing-qa`.brm_ods.discount_t dsc   
on EBI_product_obj_id0 = dsc.poid_id0  
LEFT OUTER JOIN  
    `rax-landing-qa`.brm_ods.rax_fastlane_attributes_t fastlane 
ON EBI_offering_obj_id0=fastlane.OFFERING_OBJ_ID0   
)  ;
END;
