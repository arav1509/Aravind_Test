CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_dedicated_brm_rev_daily_incremental_audit`()
BEGIN

create or replace table  stage_one.raw_dedicated_item_poid_id0_audit as 
SELECT DISTINCT 
    BILL_POID_ID0,
    C.POID_ID0		    AS ITEM_POID_ID0,
    A.BILL_NO,
    A.AR_BILL_OBJ_ID0,
    ACCOUNT_POID_ID0,
    CURRENT_DATE()		    AS Load_date,
	Bill_Created_Date
FROM 
  stage_one.raw_brm_dedicated_invoice_aggregate_total A 
INNER JOIN
    stage_one.raw_brm_dedicated_invoice_line_item_audit B 
ON A.BILL_NO=B.BILL_NO
INNER JOIN
    stage_one.raw_dedicated_brm_items C
ON A.BILL_POID_ID0=C.Bill_Obj_Id0;



--********************************************************************************************************************* 

create or replace table stage_one.raw_dedicated_bill_missing as 
SELECT DISTINCT 
    BILL_POID_ID0,
    C.POID_ID0		    AS ITEM_POID_ID0,
    A.BILL_NO,
    A.AR_BILL_OBJ_ID0,
    ACCOUNT_POID_ID0,
    CURRENT_DATE()		    AS Load_date,
	Bill_Created_Date
FROM 
   stage_one.raw_brm_dedicated_invoice_aggregate_total A 
INNER JOIN
    stage_one.raw_dedicated_brm_items C
ON A.BILL_POID_ID0=C.Bill_Obj_Id0
WHERE
    lower(A.BILL_NO) not like '%evapt%'
AND `Exclude`=0
AND not exists (SELECT BILL_NO FROM stage_two_dw.stage_dedicated_inv_event_detail B  where A.BILL_NO = B.BILL_NO);
-------------------------------------------------------------------------------------------------------------------

INSERT INTO
    stage_one.raw_dedicated_item_poid_id0_audit
SELECT DISTINCT 
    BILL_POID_ID0,
    ITEM_POID_ID0,
    BILL_NO,
    AR_BILL_OBJ_ID0,
    ACCOUNT_POID_ID0,
    CURRENT_DATE()			   AS Tbl_Load_Date,
	Bill_Created_Date
FROM 
   stage_one.raw_dedicated_bill_missing A 
WHERE
    NOT EXISTS (SELECT ITEM_POID_ID0 FROM  stage_one.raw_dedicated_item_poid_id0_audit B  where A.ITEM_POID_ID0 = b.ITEM_POID_ID0) ;
	
	
CREATE OR REPLACE TABLE     stage_one.raw_daily_dedicated_credit_poid_audit_staging AS 
SELECT DISTINCT
    Event_POID_ID0,
    Item_POID_ID0,
    ITEM_NO
FROM 
    stage_one.raw_dedicated_brm_credits_aggregate   A
INNER JOIN
    (
		SELECT --#Daily_CREDIT_POID_Audit_staging
			AGG.ITEM_NO	AS AGG_ITEM_NO, 
			AGG.TOTAL		AS AGG_TOTAL,
			CB.ITEM_NO		AS CB_ITEM_NO, 
			CB.TOTAL		AS CB_TOTAL
		FROM
		(select 
			ITEM_NO, 
			sum(TOTAL) as TOTAL 
		FROM 
			stage_one.raw_dedicated_brm_credits_aggregate 
		WHERE 
			Item_total<>0
		 group by ITEM_NO) AS AGG
		INNER JOIN
		(SELECT 
			Trx_Number AS ITEM_NO, 
			sum(TOTAL) AS TOTAL  
		from  
			stage_two_dw.stage_dedicated_inv_event_detail 
		WHERE
			LOWER(ITEM_TYPE)='/item/adjustment'
		AND LOWER(event_Type) like ('%adjustment%')
		AND TOTAL <>0
		group by 
			Trx_Number) AS CB
		ON AGG.ITEM_NO=CB.ITEM_NO
		WHERE 
			AGG.TOTAL <> CB.TOTAL
	) B--	#Daily_CREDIT_POID_Audit_staging  B
ON A.ITEM_NO =B.AGG_ITEM_NO    
WHERE
    IfNULL(Item_total,TOTAL) <> 0   ;
--*********************************************************************************************************************
SELECT DISTINCT --#Missing_Credits
    Event_POID_ID0,
    Item_POID_ID0,
    ITEM_NO
   
FROM 
    stage_one.raw_dedicated_brm_credits_aggregate  A
WHERE
     CAST(ifnull(Item_total,TOTAL) as numeric)<> 0   
AND not exists 
(SELECT 
    Trx_Number  
FROM 
    stage_two_dw.stage_dedicated_inv_event_detail B  
WHERE 
    B.Trx_Number = A.ITEM_NO
AND LOWER(ITEM_TYPE)='/item/adjustment'
AND LOWER(event_Type) like ('%adjustment%')    
    );

INSERT INTO    stage_one.raw_dedicated_item_poid_id0_audit
SELECT DISTINCT 
    0				   AS BILL_POID_ID0,
    ITEM_POID_ID0,
    ITEM_NO			   AS BILL_NO,
    0				   AS AR_BILL_OBJ_ID0,
    0				   AS ACCOUNT_POID_ID0,
    CURRENT_DATE()		   AS Tbl_Load_Date,
	0				   AS Bill_Created_Date
FROM 
    (
		SELECT DISTINCT --#Missing_Credits
			Event_POID_ID0,
			Item_POID_ID0,
			ITEM_NO
		FROM 
			stage_one.raw_dedicated_brm_credits_aggregate  A
		WHERE
			 CAST(ifnull(Item_total,TOTAL) as numeric)<> 0   
		AND not exists 
		(SELECT 
			Trx_Number  
		FROM 
			stage_two_dw.stage_dedicated_inv_event_detail B  
		WHERE 
			B.Trx_Number = A.ITEM_NO
		AND LOWER(ITEM_TYPE)='/item/adjustment'
		AND LOWER(event_Type) like ('%adjustment%')    
			)
	) A --#Missing_Credits  A
WHERE
    not exists (SELECT Event_Poid FROM stage_two_dw.stage_dedicated_inv_event_detail B  WHERE B.Event_Poid = A.Event_POID_ID0);

create or replace table stage_one.raw_dedicated_item_audit_step1
as    
SELECT DISTINCT 
    ACCOUNT_OBJ_ID0,
    POID_ID0										AS ITEM_POID_ID0,  
    Bill_Obj_Id0									AS ITEM_Bill_Obj_Id0,
    item_no										AS ITEM_NO,
    --CAST(dateadd(ss,effective_t, '1970-01-01') as date)	AS ITEM_EFFECTIVE_DATE ,    
	cast(DATETIME_ADD('1970-01-01', INTERVAL cast(effective_t as int64) SECOND) as date) as item_effective_date ,	
    --CAST(dateadd(ss,mod_t, '1970-01-01') as date)			AS ITEM_MOD_DATE , 
	cast(DATETIME_ADD('1970-01-01', INTERVAL cast(mod_t as int64) SECOND) as date)           as item_mod_date 	,
    NAME											AS ITEM_NAME,      
    STATUS										AS ITEM_STATUS, 
    SERVICE_OBJ_TYPE								AS SERVICE_OBJ_TYPE,      
    POID_TYPE										AS ITEM_TYPE,
    CURRENT_DATE()										AS Tbl_Load_Date,
    Item_gl_segment									AS ITEM_GL_SEGMENT,
	Bill_Created_Date
FROM (
SELECT DISTINCT
    i.POID_ID0,  
    I.Bill_Obj_Id0,
    i.item_no,
    i.effective_t,  
    i.ACCOUNT_OBJ_ID0,
    i.gl_segment					AS Item_gl_segment,
    i.mod_t,      
    i.NAME,      
    i.STATUS,      
    i.SERVICE_OBJ_TYPE,      
    i.POID_TYPE,
	E.Bill_Created_Date
FROM
   stage_one.raw_dedicated_item_poid_id0_audit E  
INNER JOIN
    `rax-landing-qa`.brm_ods.item_t  i   
ON  E.ITEM_POID_ID0=I.POID_ID0 
inner join
    `rax-landing-qa`.brm_ods.account_t a 
on i.ACCOUNT_OBJ_ID0 = a.poid_id0
where
    I.ITEM_TOTAL<>0
and account_no like '030%'
);


create or replace table  stage_one.raw_dedicated_event_list_audit_stage as 
SELECT
*
FROM (
SELECT 
    DI.ITEM_POID_ID0
    ,e.POID_DB
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
	,DI.Bill_Created_Date
from
	stage_one.raw_dedicated_item_audit_step1 DI
INNER JOIN
    `rax-landing-qa`.brm_ods.event_t e  
ON DI.ITEM_POID_ID0=e.Item_Obj_Id0 
);

create or replace table stage_one.raw_dedicated_event_list_audit_archive_step  as 
SELECT
*
FROM (
SELECT 
    DI.ITEM_POID_ID0
    ,e.POID_DB
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
	,DI.Bill_Created_Date
from
	stage_one.raw_dedicated_item_audit_step1 DI
INNER JOIN
    `rax-landing-qa`.brm_ods.event_t_archive e  
ON DI.ITEM_POID_ID0=e.Item_Obj_Id0 
);

INSERT INTO
	stage_one.raw_dedicated_event_list_audit_stage 
SELECT
*
FROM	stage_one.raw_dedicated_event_list_audit_archive_step
WHERE
	POID_ID0  NOT IN (SELECT DISTINCT POID_ID0 FROM stage_one.raw_dedicated_event_list_audit_stage) ;

create or replace table   stage_one.raw_dedicated_event_list_audit_unpar_archive_step as 
SELECT
*
FROM (
SELECT 
    DI.ITEM_POID_ID0
    ,e.POID_DB
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
	,DI.Bill_Created_Date
from
	stage_one.raw_dedicated_item_audit_step1 DI
INNER JOIN
    `rax-landing-qa`.brm_ods.event_t__unpartitioned_archive e  
ON DI.ITEM_POID_ID0=e.Item_Obj_Id0 
);


INSERT INTO 	stage_one.raw_dedicated_event_list_audit_stage 
SELECT
*
FROM
	stage_one.raw_dedicated_event_list_audit_unpar_archive_step
WHERE
	POID_ID0  NOT IN (SELECT DISTINCT POID_ID0 FROM  stage_one.raw_dedicated_event_list_audit_stage) ;
	

CREATE OR REPLACE TABLE   stage_one.raw_dedicated_event_list_audit_step as
SELECT 
     ITEM_POID_ID0
    ,e.POID_DB
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
	,e.Bill_Created_Date
from
	stage_one.raw_dedicated_event_list_audit_stage  e
INNER JOIN
	`rax-landing-qa`.brm_ods.account_t acct --#ACCOUNT_T acct 
ON e.ACCOUNT_OBJ_ID0=acct.POID_ID0 
WHERE
	account_no like '030%'
AND (e.batch_id IS NULL  OR e.batch_Id	not like 'Rerating%')
AND e.rerate_obj_id0 = 0
AND (
	LOWER(e.poid_type) like '/event/activity/Rax/dedicated%'
OR  LOWER(e.poid_type) like '%adjustment%'
OR  LOWER(e.poid_type) like '/event/delayed/rax/dedicated%'
OR	LOWER(e.poid_type) ='/event/delayed/rax/dedicated/infrastructure/uptime'
OR	LOWER(e.poid_type) ='/event/delayed/rax/dedicated/infrastructure/services'
OR	LOWER(e.poid_type) = '/event/billing/cycle/tax'
OR	LOWER(e.poid_type) = '/event/billing/cycle/discount' 
OR	LOWER(e.poid_type) = '/event/billing/cycle/fold'
OR	LOWER(e.poid_type) = '/event/billing/product/fee/purchase'
OR	LOWER(e.poid_type) = '/event/activity/Rax/fastlane'
OR	LOWER(e.poid_type) like '/event/billing/product/fee/cycle/cycle%'
OR  LOWER(e.SERVICE_OBJ_TYPE) like  '%/rax/fastlane/%'
OR  LOWER(e.SERVICE_OBJ_TYPE) like '%datapipe%'
);

CREATE OR REPLACE TABLE stage_one.raw_dedicated_audit_fast_lane AS 
SELECT
    fastlane_EVENT_POID_ID0,
    INV_GRP_CODE										AS ACTIVITY_SERVICE_TYPE,   --new ield added 1.20.2017 kvc
    INV_SUB_GRP_CODE									AS ACTIVITY_EVENT_TYPE,		--new ield added 1.20.2017 kvc
    RECORD_ID											AS ACTIVITY_RECORD_ID,		--new ield added 1.20.2017 kvc
    DATA_CENTER_ID										AS ACTIVITY_DC_ID,			--new ield added 1.20.2017 kvc
    REGION											AS ACTIVITY_REGION,			--new ield added 1.20.2017 kvc
    resource_id										AS ACTIVITY_RESOURCE_ID,	--new ield added 1.20.2017 kvc
    resource_name										AS ACTIVITY_RESOURCE_NAME,  --new ield added 1.20.2017 kvc
    attr1												AS ACTIVITY_ATTR1,			--new ield added 1.20.2017 kvc
    attr2												AS ACTIVITY_ATTR2,			--new ield added 1.20.2017 kvc
    attr3												AS ACTIVITY_ATTR3,			--new ield added 1.20.2017 kvc
    CURRENT_DATE()											AS Tbl_Load_Date,
    UOM												AS ACTIVITY_UOM,
    ifnull(backbill_flag,0)							     AS ACTIVITY_backbill_flag,
	Attr4 AS ACTIVITY_ATTR4,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	Attr5 AS ACTIVITY_ATTR5,   --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	Attr6 AS ACTIVITY_ATTR6    --new field added 7.10.2019 anil2912 as per uday request for NRD Staging
FROM
    (
SELECT DISTINCT
    E.POID_ID0						AS fastlane_EVENT_POID_ID0,
    fastlane.INV_GRP_CODE,			--new ield added 1.20.2017 kvc
    fastlane.INV_SUB_GRP_CODE,		--new ield added 1.20.2017 kvc
    fastlane.RECORD_ID,				--new ield added 1.20.2017 kvc
    fastlane.DATA_CENTER_ID,			--new ield added 1.20.2017 kvc
    fastlane.REGION,				--new ield added 1.20.2017 kvc
    fastlane.resource_id,			--new ield added 1.20.2017 kvc
    fastlane.resource_name,			--new ield added 1.20.2017 kvc
    fastlane.attr1,					--new ield added 1.20.2017 kvc
    fastlane.attr2,					--new ield added 1.20.2017 kvc
    fastlane.attr3,					--new ield added 1.20.2017 kvc
    fastlane.UOM,				     --new ield added 4.18.2019 kvc				 
    BACKBILL_FLAG					 AS backbill_flag,
	fastlane.Attr4,    
	fastlane.Attr5,    
	fastlane.Attr6 
FROM
    stage_one.raw_dedicated_event_list_audit_step e   
INNER JOIN  
    `rax-landing-qa`.brm_ods.event_act_rax_fastlane_t AS fastlane 
ON E.poid_id0 = fastlane.obj_id0 
); 



create or replace table    stage_one.raw_dedicated_event_audit_step3 as 
SELECT DISTINCT
    poid_id0						    AS EVENT_POID_ID0,
    EVENT_account_poid,
    Item_Obj_Id0					    AS EVENT_Item_Obj_Id0,
    poid_type						    AS EVENT_type,  
	DATETIME_ADD('1970-01-01', INTERVAL cast(created_t as int64) SECOND) AS EVENT_create_dtt,
	DATETIME_ADD('1970-01-01', INTERVAL cast(mod_t as int64) SECOND)       as event_mod_dtt,
    DATETIME_ADD('1970-01-01', INTERVAL cast(start_t as int64) SECOND) as event_start_dtt,
    DATETIME_ADD('1970-01-01', INTERVAL cast(end_t as int64) SECOND)  as event_end_dtt,
    descr							    AS EVENT_descr,
    EVENT_name,
    program_name					    AS EVENT_program_name,
    rum_name						    AS EVENT_rum_name,
    sys_descr						    AS EVENT_sys_descr,
    usage_type						    AS EVENT_usage_type,
    invoice_data					    AS EVENT_invoice_data,
    EVENT_service_obj_id0,
    service_obj_type				    AS EVENT_service_obj_type,
    SESSION_OBJ_ID0				    AS EVENT_Session_obj_ido,
    rerate_obj_id0					    AS EVENT_rerate_obj_id0,
    resource_id					    AS BIL_CYC_resource_id,
    quantity						    AS BIL_CYC_quantity,	--- no values
    SERVICE_login_site_id,
    LOGIN						    AS SERVICE_obj_id0,		--aka site id/bill_site_id
    SERVICE_name,
    TYPE						    AS service_type_classid,
    SERVICE_Acct_poid,
    BAL_GRP_OBJ_ID0				    AS SERVICE_BalGrp_poid,
    STATUS							  AS SERVICE_Status,
    DATETIME_ADD('1970-01-01', INTERVAL cast(effective_t as int64) SECOND)    as service_eff_date,
    CURRENT_DATE()								  AS Tbl_Load_Date,
    DATETIME_ADD('1970-01-01', INTERVAL cast(earned_start_t as int64) SECOND)      as event_earned_start_dtt,
    DATETIME_ADD('1970-01-01', INTERVAL cast(earned_end_t as int64) SECOND)   as event_earned_end_dtt,
	Bill_Created_Date
FROM (
SELECT  
    e.poid_id0,
    e.ACCOUNT_OBJ_ID0				AS EVENT_account_poid,
    e.Item_Obj_Id0,
    e.poid_type,
    e.created_t,
    e.mod_t,
    e.start_t, 
    e.end_t, 
    e.EARNED_START_T,
    e.EARNED_END_T,
    e.descr,
    e.name						AS EVENT_name,
    e.program_name,
    e.rum_name,
    e.sys_descr,
    e.usage_type,
    e.invoice_data,
     e.service_obj_id0				AS EVENT_service_obj_id0,
    e.service_obj_type,
    e.SESSION_OBJ_ID0,
    e.rerate_obj_id0,
    e.dw_Timestamp,
    bil_cyc_fld.resource_id,
    bil_cyc_fld.quantity,	
    service_obj_id0					AS SERVICE_login_site_id,
    svc.LOGIN,		
    svc.NAME						AS SERVICE_name,
    svc.TYPE,
    svc.ACCOUNT_OBJ_ID0				AS SERVICE_Acct_poid,
    svc.BAL_GRP_OBJ_ID0,
    svc.STATUS,
    svc.EFFECTIVE_T,
	e.Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
FROM
   stage_one.raw_dedicated_event_list_audit_step e   
LEFT OUTER JOIN
    `rax-landing-qa`.brm_ods.event_billing_cycle_fold_t AS  bil_cyc_fld        
ON e.poid_id0 = bil_cyc_fld.Obj_Id0 
LEFT OUTER JOIN	
   `rax-landing-qa`.brm_ods.service_t as svc 
on e.service_obj_id0 = svc.POID_ID0
)  ;


create or replace table stage_one.raw_dedicated_adj_audit_step2 as 
SELECT DISTINCT
    EVENT_POID_ID0,
    MISC_EVENT_BILLING_Type, ---Credit_Type
    MISC_EVENT_BILLING_Type_Reason_ID, --Credit_Reason_ID
    MISC_EVENT_BILLING_Type_record_id,
    string_domain,
    string_version,
    CURRENT_DATE()					 AS Tbl_Load_Date
FROM (
SELECT
    E.poid_id0  AS EVENT_POID_ID0,
    STRING	  AS MISC_EVENT_BILLING_Type, ---Credit_Type
    REASON_ID	  AS MISC_EVENT_BILLING_Type_Reason_ID, --Credit_Reason_ID
    rec_id	  AS MISC_EVENT_BILLING_Type_record_id,
    domain	  AS string_domain,
    version	  AS string_version
FROM
   stage_one.raw_dedicated_event_list_audit_step e    
LEFT OUTER JOIN
    `rax-landing-qa`.brm_ods.event_billing_misc_t EBM  
ON E.poid_id0 = EBM.OBJ_ID0 
LEFT OUTER JOIN
    `rax-landing-qa`.brm_ods.strings_t  S  
ON EBM.REASON_ID = S.STRING_ID 
AND EBM.REASON_DOMAIN_ID = S.VERSION 
and lower(s.domain) LIKE 'reason%'  ) ;



create or replace table stage_one.raw_dedicated_rax_events_audit_load_step4  as 
SELECT DISTINCT 
    obj_id0							  AS DED_obj_id0,
    quantity							  AS DED_quantity,
    orig_Quantity						  AS DED_orig_Quantity,
    rax_uom							  AS DED_rax_uom, 
    ATTR2								  AS DED_period_name,
	DATETIME_ADD('1970-01-01', INTERVAL cast(inv_date_t as int64) SECOND) as ded_inv_date,
    usage_type							  AS DED_usage_type,
    billing_type						  AS DED_billing_type,
    device_id							  AS DED_device_id,
    device_name						  AS DED_device_name,	
    DATETIME_ADD('1970-01-01', INTERVAL cast(billing_start_t as int64) SECOND)  as ded_bill_start,
    DATETIME_ADD('1970-01-01', INTERVAL cast(billing_end_t as int64) SECOND)   as ded_bill_end,  
    DATETIME_ADD('1970-01-01', INTERVAL cast(prepay_start_t as int64) SECOND)  as ded_prepay_start,    
    DATETIME_ADD('1970-01-01', INTERVAL cast(prepay_end_t as int64) SECOND) as ded_prepay_end, 
    batch_id							  AS DED_batch_id,
    login							  AS DED_login_siteid,
    product_name						  AS DED_product_name,
    attr1								  AS DED_prod_type,
    ATTR3								  AS DED_Event_Desc,
    ATTR4 							  AS DED_Make_Model,
    ATTR5								  AS DED_OS,
    ATTR6								  AS DED_RAM,
    ATTR7								  AS DED_Processor,
    record_id							  AS DED_record_id,
    data_center_id						  AS DED_data_center_id,
    region							  AS DED_region,
    resource_id						  AS DED_currency_id,
    CURRENT_DATE()							  AS Tbl_Load_Date
FROM (
SELECT  
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
    stage_one.raw_dedicated_event_audit_step3 A
INNER JOIN
    `rax-landing-qa`.brm_ods.event_act_rax_dedicated_t eded      
on  A.EVENT_POID_ID0 = eded.obj_id0
);

create or replace table stage_one.raw_dedicated_impacts_audit_load_step5_initial  as 
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
    CURRENT_DATE()			   AS Tbl_Load_Date,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
	Tax_Type_Id,
	Tax_Element_Id,
	Bill_Created_Date
FROM (
SELECT  
    ebi.OBJ_ID0		   AS EBI_OBJ_ID0,     
    ebi.rec_id			   AS EBI_Rec_id,
    ebi.amount			   AS EBI_amount,
    ebi.amount_orig		   AS EBI_amount_Orig,
    ebi.gl_id			   AS EBI_GL_ID,
    ebi.resource_ID		   AS EBI_CURRENCY_ID,
    ebi.impact_category	   AS EBI_impact_category,
    ebi.impact_type		   AS EBI_impact_type,
    ebi.product_obj_id0	   AS EBI_product_obj_id0,
    ebi.product_obj_type    AS EBI_product_obj_type,
    ebi.quantity		   AS EBI_quantity,
    ebi.rate_tag		   AS EBI_rate_tag,
    ebi.resource_id		   AS EBI_resource_id,
    ebi.tax_code		   AS EBI_tax_code,
    ebi.rum_id			   AS EBI_rum_id,
    ebi.discount		   AS EBI_discount,
    ebi.offering_obj_id0    AS EBI_offering_obj_id0,
    ebi.offering_obj_type   AS EBI_offering_obj_type,
    ebi.BAL_GRP_OBJ_ID0   AS EBI_bal_grp_obj_ido,
    ebi.BAL_GRP_OBJ_TYPE  AS EBI_bal_grp_obj_type,
    fastlane_invoicemap.IMPACT_Key		 AS FASTLANE_IMPACT_CATEGORY,	--new field added 11.2.2018 kvc
    fastlane_invoicemap.IMPACT_VALUE	 AS FASTLANE_IMPACT_VALUE,	--new field added 11.2.2018 kvc
	etj.Type							 AS Tax_Type_Id,   --new field added 7.23.2019
	etj.ELEMENT_ID						 AS Tax_Element_Id, --new field added 7.23.2019
	A.Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging

FROM
   stage_one.raw_dedicated_event_audit_step3 A 
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


create or replace table  stage_one.raw_dedicated_impacts_audit_load_step5_archive_initial as 
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
    CURRENT_DATE()			   AS Tbl_Load_Date,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
	Tax_Type_Id,
	Tax_Element_Id,
	Bill_Created_Date
FROM (
SELECT 
    ebi.OBJ_ID0		   AS EBI_OBJ_ID0,     
    ebi.rec_id			   AS EBI_Rec_id,
    ebi.amount			   AS EBI_amount,
    ebi.amount_orig		   AS EBI_amount_Orig,
    ebi.gl_id			   AS EBI_GL_ID,
    ebi.resource_ID		   AS EBI_CURRENCY_ID,
    ebi.impact_category	   AS EBI_impact_category,
    ebi.impact_type		   AS EBI_impact_type,
    ebi.product_obj_id0	   AS EBI_product_obj_id0,
    ebi.product_obj_type    AS EBI_product_obj_type,
    ebi.quantity		   AS EBI_quantity,
    ebi.rate_tag		   AS EBI_rate_tag,
    ebi.resource_id		   AS EBI_resource_id,
    ebi.tax_code		   AS EBI_tax_code,
    ebi.rum_id			   AS EBI_rum_id,
    ebi.discount		   AS EBI_discount,
    ebi.offering_obj_id0    AS EBI_offering_obj_id0,
    ebi.offering_obj_type   AS EBI_offering_obj_type,
    ebi.BAL_GRP_OBJ_ID0   AS EBI_bal_grp_obj_ido,
    ebi.BAL_GRP_OBJ_TYPE  AS EBI_bal_grp_obj_type,
    fastlane_invoicemap.IMPACT_Key		 AS FASTLANE_IMPACT_CATEGORY,	--new field added 11.2.2018 kvc
    fastlane_invoicemap.IMPACT_VALUE	 AS FASTLANE_IMPACT_VALUE,	--new field added 11.2.2018 kvc
	etj.Type							 AS Tax_Type_Id,   --new field added 7.23.2019
	etj.ELEMENT_ID						 AS Tax_Element_Id, --new field added 7.23.2019
	A.Bill_Created_Date									--new field added 7.31.2019 rahu4260 as per uday request for NRD Staging  

FROM
   stage_one.raw_dedicated_event_audit_step3 A 
INNER JOIN 
   `rax-landing-qa`.brm_ods.event_bal_impacts_t_archive  ebi 
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


INSERT INTO  stage_one.raw_dedicated_impacts_audit_load_step5_initial 
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
    CURRENT_DATE()			   AS Tbl_Load_Date,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
	Tax_Type_Id,
	Tax_Element_Id,
	Bill_Created_Date
FROM 
    stage_one.raw_dedicated_impacts_audit_load_step5_archive_initial
WHERE
     EBI_OBJ_ID0  NOT IN (SELECT DISTINCT EBI_OBJ_ID0 FROM  stage_one.raw_dedicated_impacts_audit_load_step5_initial);

create or replace table stage_one.raw_dedicated_impacts_audit_load_step5_unpar_archive_initial  as 
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
    CURRENT_DATE()			   AS Tbl_Load_Date,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
	Tax_Type_Id,
	Tax_Element_Id,
	Bill_Created_Date
FROM (
SELECT 
    ebi.OBJ_ID0					AS EBI_OBJ_ID0,     
    ebi.rec_id						AS EBI_Rec_id,
    ebi.amount						AS EBI_amount,
    ebi.amount_orig					AS EBI_amount_Orig,
    ebi.gl_id						AS EBI_GL_ID,
    ebi.resource_ID					AS EBI_CURRENCY_ID,
    ebi.impact_category				AS EBI_impact_category,
    ebi.impact_type					AS EBI_impact_type,
    ebi.product_obj_id0				AS EBI_product_obj_id0,
    ebi.product_obj_type				AS EBI_product_obj_type,
    ebi.quantity					AS EBI_quantity,
    ebi.rate_tag					AS EBI_rate_tag,
    ebi.resource_id					AS EBI_resource_id,
    ebi.tax_code					AS EBI_tax_code,
    ebi.rum_id						AS EBI_rum_id,
    ebi.discount					AS EBI_discount,
    ebi.offering_obj_id0				AS EBI_offering_obj_id0,
    ebi.offering_obj_type			AS EBI_offering_obj_type,
    ebi.BAL_GRP_OBJ_ID0			AS EBI_bal_grp_obj_ido,
    ebi.BAL_GRP_OBJ_TYPE			AS EBI_bal_grp_obj_type,
    fastlane_invoicemap.IMPACT_Key		AS FASTLANE_IMPACT_CATEGORY,	--new field added 11.2.2018 kvc
    fastlane_invoicemap.IMPACT_VALUE	AS FASTLANE_IMPACT_VALUE,	--new field added 11.2.2018 kvc
	etj.Type							 AS Tax_Type_Id,   --new field added 7.23.2019
	etj.ELEMENT_ID						 AS Tax_Element_Id, --new field added 7.23.2019
	A.Bill_Created_Date											--new field added 7.31.2019 rahu4260 as per uday request for NRD Staging

FROM
   stage_one.raw_dedicated_event_audit_step3 A 
INNER JOIN 
   `rax-landing-qa`.brm_ods.event_bal_impacts_t__unpartitioned_archive  ebi 
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
--*********************************************************************************************************************
--CREATE INDEX IX_EBI_OBJ_ID0 ON Raw_Dedicated_Impacts_Audit_Load_Step5_Unpar_Archive_Initial (EBI_OBJ_ID0) /*RV 2019/04/18 commented*/
--*********************************************************************************************************************
INSERT INTO  stage_one.raw_dedicated_impacts_audit_load_step5_initial 
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
    CURRENT_DATE()			   AS Tbl_Load_Date,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
	Tax_Type_Id,
	Tax_Element_Id,
	Bill_Created_Date
FROM 
    stage_one.raw_dedicated_impacts_audit_load_step5_unpar_archive_initial
WHERE
     EBI_OBJ_ID0  NOT IN (SELECT DISTINCT EBI_OBJ_ID0 FROM  stage_one.raw_dedicated_impacts_audit_load_step5_initial);


create or replace table   stage_one.raw_dedicated_impacts_audit_load_step6 as 
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
    CURRENT_DATE()						    AS Tbl_Load_Date,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
    FASTLANE_INV_DEAL_CODE,
    FASTLANE_INV_GRP_CODE,
    FASTLANE_INV_SUB_GRP_CODE,
    ifnull(FASTLANE_INV_Is_Backbill,0)	    AS FASTLANE_INV_Is_Backbill,
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
    cur.currency							  AS currency_abbrev,
    cur.name								  AS currency_name,
    erm.rec_id								  AS erm_rec_id,
    erm.rum_name							  AS erm_rum_name,	
    ifnull( prd.poid_id0, dsc.poid_id0)			  AS product_poid,
    ifnull(prd.poid_type, dsc.poid_type)		  AS product_type, 
    ifnull( prd.descr, dsc.descr)				  AS product_descr,  
    ifnull( prd.name, dsc.name)				  AS product_name, 
    ifnull( prd.code, dsc.code)				  AS product_code, 
    ifnull( prd.permitted, dsc.permitted)		  AS prod_permitted,
    ifnull( prd.type, dsc.type)			  AS product_type2,
    FASTLANE_IMPACT_CATEGORY,
    FASTLANE_IMPACT_VALUE,
    DEAL_CODE								  AS FASTLANE_INV_DEAL_CODE,
    INV_GRP_CODE							  AS FASTLANE_INV_GRP_CODE,
    INV_SUB_GRP_CODE						  AS FASTLANE_INV_SUB_GRP_CODE,
    BACKBILL_FLAG							  AS FASTLANE_INV_Is_Backbill,
	Tax_Type_Id,							 --new field added 7.23.2019 
	Tax_Element_Id,							 --new field added 7.23.2019 
	Bill_Created_Date					     --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
FROM
    stage_one.raw_dedicated_impacts_audit_load_step5_initial A 
LEFT OUTER JOIN
    `rax-landing-qa`.brm_ods.ifw_currency  CUR  
ON EBI_resource_id = CUR.CURRENCY_ID
LEFT OUTER JOIN
    `rax-landing-qa`.brm_ods.event_rum_map_t	 AS erm          
on A.EBI_OBJ_ID0= Erm.Obj_Id0		
LEFT OUTER JOIN
    `rax-landing-qa`.brm_ods.product_t prd 
on EBI_product_obj_id0 = prd.poid_id0
LEFT OUTER JOIN
    `rax-landing-qa`.brm_ods.discount_t dsc 
on EBI_product_obj_id0 = dsc.poid_id0
LEFT OUTER JOIN
    `rax-landing-qa`.brm_ods.rax_fastlane_attributes_t fsa 
on EBI_offering_obj_id0 = fsa.offering_obj_id0
);


END;
