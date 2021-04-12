CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_stage_dedicated_inv_event_detail_incremental`()
BEGIN



------
create or replace temp table INV_DETAIL_A as  
select 
 master_unique_id
,Trx_ID
,Trx_Line_ID
,Trx_Line_GL_Dist_ID
,Trx_Resource_ID
,Trx_Number
,Trx_Complete_Flag
,Trx_Bill_Pay_Site_ID
,Invoice_WKST_SNID
,Trx_Unit_Of_Measure_Code
,case when Trx_Raw_Start_Date is null then upper(FORMAT_DATE("%b-%y", cast(Invoice_Date as date)))
	else Trx_Raw_Start_Date
 end as Trx_Raw_Start_Date
,Trx_Qty
,Trx_Term
,Trx_Description
,case when if(SAFE_CAST(Account AS FLOAT64) is null,'FALSE', 'TRUE') <> 'FALSE'
      and length(bq_functions.udf_stripnonnumeric(Account)) >3
	then bq_functions.udf_stripnonnumeric(Account)
	else Account
 end as Account 
,Server
,Server_Make_Model
,Server_OS
,Server_RAM
,Server_Processor
,Organization
,Oracle_Department_ID
,Oracle_Department
,Oracle_Product_ID
,Oracle_Product
,Oracle_Business_Unit_ID
,Oracle_Business_Unit
,Oracle_Team_ID
,Oracle_Team
,Oracle_Company_ID
,Oracle_Company
,Oracle_Location_ID
,Oracle_Location
,GL_Account
,Oracle_GL_Account_DESC
,DED_usage_type
,Dedicated_Product_Group
,Dedicated_Product_Type
,Invoice_Date
,Time_Month_Key
,TOTAL
,Extended_Amount
,Billing_Days_In_Month
,Currency_ID
,Currency_abbrev
,Currency_name
,Refresh_Date
,print_group
,Trx_Date
,Unit_Selling_Price
,EVENT_POID_ID0
,EVENT_Item_Obj_Id0
,EVENT_mod_dtt
,EBI_Rec_id
,EVENT_type
,ITEM_TYPE
,ITEM_POID_ID0
,ITEM_NO
,ITEM_name
,Item_Effective_Date
,ITEM_Bill_Obj_Id0
,EVENT_service_obj_type
,EVENT_service_obj_id0
,EBI_GL_ID
,product_poid
,product_type
,product_descr
,product_name
,product_code
,prod_permitted
,product_type2
,EVENT_Session_obj_ido
,MISC_EVENT_BILLING_Type
,MISC_EVENT_BILLING_Type_Reason_ID
,MISC_EVENT_BILLING_Type_record_id
,string_domain
,string_version
,SERVICE_login_site_id
,DED_login_siteid
,BRM_ACCOUNT_NO
,account_poid
,PAYMENT_TERM
,GL_SEGMENT
,Bill_NO
,BILL_END_DATE
,BILL_START_DATE
,BILL_MOD_DATE
,DED_bill_start
,DED_bill_end
,DED_prepay_start
,DED_prepay_end
,BUSINESS_TYPE
,BUSINESS_TYPE_DESCR
,DED_record_id
,DED_region
,profile_bu
,master_tbl_loaddtt
,LINE_OF_BUSINESS
,Event_Create_Date
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
,IMPACT_CATEGORY
,IMPACT_TYPE
,EVENT_earned_start_dtt
,EVENT_earned_end_dtt
,EVENT_FASTLANE_IS_BACKBILL
,FASTLANE_INV_DEAL_CODE
,FASTLANE_INV_GRP_CODE
,FASTLANE_INV_SUB_GRP_CODE
,FASTLANE_INV_Is_Backbill
,EBI_OFFERING_OBJ_ID0
,Event_Start_dtt
,EVENT_end_dtt
,ACTIVITY_ATTR4
,ACTIVITY_ATTR5
,ACTIVITY_ATTR6
,EBI_rate_tag
,Tax_Type_Id
,Tax_Element_Id
,RATE
,Impact_Type_Description
,Bill_Created_Date
from(
SELECT --INTO     #INV_DETAIL_A 
    master_unique_id,
   IFNULL(ROUND( CAST(CONCAT(BILL_POID,'.',ITEM_POID_ID0)AS NUMERIC),3),0) AS Trx_ID,
    EVENT_POID_ID0 AS Trx_Line_ID,
    master_unique_id AS Trx_Line_GL_Dist_ID,  -- most unique id for record
    '0' AS Trx_Resource_ID,-- no equivalent to this value for join to other oracle data_jcm
    IFNULL(IFNULL(DED.bill_no,ITEM_NO),'Uknown') AS Trx_Number,
   'Y' AS Trx_Complete_Flag,-- any record in brm is considered 'complete' billing'
   CAST( IFNULL(service_login_site_id, cast(DED_login_siteid as NUMERIC)) as NUMERIC) AS Trx_Bill_Pay_Site_ID,-- logic for this awaiting amit/silpa input - should tie to login id for bal group record?
    '0' AS Invoice_WKST_SNID,-- do not know what this value represents or is used ofr_jcm
    IFNULL(DED_rax_uom,'Ea') AS Trx_Unit_Of_Measure_Code, --default to ea when not present
    DED_period_name AS Trx_Raw_Start_Date,
    IFNULL(DED_orig_Quantity, 1) AS Trx_Qty,
    IFNULL(case when LOWER(DED_rax_uom) IN ('ea', 'mth') THEN 1 ELSE  IFNULL(DED_orig_Quantity, 1) END	,1) AS Trx_Term,-- no brm source for this specific value - should this use trx_qty?
    IFNULL(coalesce(DED.event_descr, EVENT_sys_descr),ITEM_name) AS Trx_Description,
    DED.ACCOUNT_NUMBER AS Account,
    IFNULL(DED_device_id,'0') AS Server,
    DED_Make_Model AS Server_Make_Model,
    DED_OS AS Server_OS,
    DED_RAM AS Server_RAM,
    DED_Processor AS Server_Processor,
    DED.ORG_VALUE AS Organization,
    cast(NULL as STRING) AS Oracle_Department_ID,  -- not found in brm data
    cast( 'Unknown'as STRING) AS Oracle_Department,-- not found in brm data
    cast(SUBSTR(EBI_impact_category, 1,4) as STRING) AS Oracle_Product_ID,-- partial data from event, else gl segs /* only ~45% of records have impact_type (tax and 'activity' recs do not have impact)*/
    cast('Unknown' as STRING) AS Oracle_Product,-- lookup 
    cast( NULL as STRING ) AS Oracle_Business_Unit_ID,-- not found in brm data
    cast('Unknown'as STRING) AS Oracle_Business_Unit,-- not found in brm data
    cast( DED.SUPPORT_TEAM as STRING ) AS Oracle_Team_ID,-- from brm ded
    cast( IFNULL(DED.SUPPORT_TEAM_DESCR, 'Unknown')as STRING) AS Oracle_Team,--from brm ded
    cast( DED.CONTRACTING_ENTITY as STRING) AS Oracle_Company_ID,-- from brm ded
    cast( DED.ORGANIZATION as STRING) AS Oracle_Company,-- from brm ded
    cast( NULL as STRING) AS Oracle_Location_ID,--from gl_segs
    cast( DED_data_center_id as STRING) AS Oracle_Location,-- from brm ded
    --------------------
    SUBSTR(EBI_impact_category,STRPOS(EBI_impact_category,'_')+1,6) AS GL_Account,  -- partial data from brm event; else gl segs /*-this vallue is only populated 45% of the time - ebi.impact_type is not provided for all event_types.....*/
    cast('Unknown' as STRING) AS Oracle_GL_Account_DESC,
    DED_usage_type,
    DED_product_name AS Dedicated_Product_Group,
    DED.ded_prod_type AS Dedicated_Product_Type,
    IFNULL(IFNULL(DED_inv_date, ded.BILL_END_DATE),EVENT_create_dtt) AS Invoice_Date,
    cast(bq_functions.udf_time_key_nohyphen(IFNULL(IFNULL(DED_inv_date, ded.BILL_END_DATE),EVENT_create_dtt)) as int64) AS Time_Month_Key,
    EBI_amount AS TOTAL,
    IFNULL(EBI_amount,0) AS Extended_Amount,  -- ??
    extract(day from date_trunc( cast(( IfNULL(ifnull(DED_inv_date, ded.BILL_END_DATE),EVENT_create_dtt)) as date) , MONTH)-1) AS Billing_Days_In_Month,
    coalesce(ded.EBI_CURRENCY_ID, cast(DED_currency_id as numeric)) AS Currency_ID,
    Currency_abbrev,
    Currency_name,
    DED.event_mod_dtt AS Refresh_Date,
    'Unknown' AS print_group,
    IFNULL(IFNULL(DED_inv_date, ded.BILL_END_DATE),EVENT_create_dtt) AS Trx_Date,
    IFNULL(EBI_amount,0) AS Unit_Selling_Price,
    ---------------------------
    EVENT_POID_ID0,
    EVENT_Item_Obj_Id0,
    EVENT_mod_dtt,
    EBI_Rec_id,
    EVENT_type,
    ITEM_TYPE,
    ITEM_POID_ID0,
    ITEM_NO,
    ITEM_name,
    Item_Effective_Date,
    ITEM_Bill_Obj_Id0,
    EVENT_service_obj_type,
    EVENT_service_obj_id0,
    EBI_GL_ID,
    product_poid,	
    product_type,	
    product_descr,	
    product_name,	
    product_code,	
    prod_permitted,
    product_type2,
    EVENT_Session_obj_ido,
    MISC_EVENT_BILLING_Type,			---Credit_Type
    MISC_EVENT_BILLING_Type_Reason_ID,  --Credit_Reason_ID
    MISC_EVENT_BILLING_Type_record_id,
    string_domain,
    string_version,     
    SERVICE_login_site_id,
    DED_login_siteid,
    BRM_ACCOUNT_NO,
    account_poid,
    PAYMENT_TERM,
    IFNULL(GL_SEGMENT,'.dedicated') AS GL_SEGMENT,
    Bill_NO,
    BILL_END_DATE,   -- used for pre-pays
    BILL_START_DATE, -- used for pre-pays
    BILL_MOD_DATE,
    DED_bill_start, -- used for pre-pays
    DED_bill_end, -- used for pre-pays
    DED_prepay_start,  -- new field coming to event dedicated data table_no target date yet_11.14.16jcm
    DED_prepay_end,  -- new field coming to event dedicated data table_no target date yet_11.14.16jcm
    BUSINESS_TYPE,
    BUSINESS_TYPE_DESCR,
    DED_record_id,
    DED_region,
    BUSINESS_UNIT AS profile_bu,
    master_tbl_loaddtt,
    LINE_OF_BUSINESS,
    EVENT_create_dtt AS Event_Create_Date,
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
    FASTLANE_IMPACT_VALUE,			-- new field added 1.20.17_kvc ,
    EBI_impact_category AS IMPACT_CATEGORY,
    EBI_IMPACT_TYPE AS IMPACT_TYPE,
    EVENT_earned_start_dtt, 
    EVENT_earned_end_dtt,
    EVENT_FASTLANE_IS_BACKBILL,
    FASTLANE_INV_DEAL_CODE,
    FASTLANE_INV_GRP_CODE,
    FASTLANE_INV_SUB_GRP_CODE,
    FASTLANE_INV_Is_Backbill,
    EBI_OFFERING_OBJ_ID0,
	Event_Start_dtt,
	EVENT_end_dtt,
	ACTIVITY_ATTR4, -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR5, -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR6,	-- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	EBI_rate_tag,    ---- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Tax_Element_Id,      --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	CASE 
		WHEN UPPER(EBI_IMPACT_CATEGORY)	like '%MANAGED%'  THEN  cast(EBI_AMOUNT as string)
		WHEN UPPER(EVENT_SERVICE_OBJ_TYPE) like '/SERVICE/RAX/CLOUD/SITE/DOMAIN'	THEN cast(EBI_AMOUNT as string)		
		WHEN UPPER(EVENT_TYPE) = '/EVENT/BILLING/CYCLE/TAX'	THEN NULL
		WHEN UPPER(EVENT_TYPE) = '/EVENT/ACTIVITY/RAX/FASTLANE' THEN '1'		
		WHEN (EBI_IMPACT_TYPE) = 1 AND UPPER(EVENT_TYPE) in ('/EVENT/BILLING/PRODUCT/FEE/CYCLE/CYCLE_FORWARD_ANNUAL','/EVENT/BILLING/PRODUCT/FEE/PURCHASE') 	THEN NULL
		WHEN EBI_RATE_TAG LIKE '%|%'    THEN SUBSTR(EBI_RATE_TAG,1, STRPOS(EBI_RATE_TAG,'|')-1)		
    END	AS  RATE,     --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	CASE 
		WHEN EBI_IMPACT_TYPE = 258 OR (EBI_IMPACT_TYPE  = 128  AND UPPER(EBI_IMPACT_CATEGORY) <>'MANAGED') OR (EBI_IMPACT_TYPE = 1 AND UPPER(EBI_IMPACT_CATEGORY) like '%LEGACY_MANAGED_FEE%' ) OR EBI_IMPACT_TYPE = 1   THEN  'CHARGE'
		WHEN EBI_IMPACT_TYPE  = 128 AND UPPER(EBI_IMPACT_CATEGORY) = 'MANAGED'  THEN 'MANAGED_FEE'
		WHEN EBI_IMPACT_TYPE  = 1   AND UPPER(EBI_IMPACT_CATEGORY) like '%LEGACY_MANAGED_FEE%' THEN  'MANAGED_FEE'	
		WHEN EBI_IMPACT_TYPE = 128  AND EBI_AMOUNT <= 0 
		THEN  ( CASE
						WHEN  UPPER(EBI_IMPACT_CATEGORY) = 'RAX_DSC'	      THEN 'DISCOUNT - Racker'
    					WHEN  UPPER(EBI_IMPACT_CATEGORY) = 'DEV_DISC'         THEN 'DISCOUNT - Developer'
						WHEN  UPPER(EBI_IMPACT_CATEGORY)  LIKE 'DEVP_DISC%'	  THEN 'DISCOUNT - Developer+'			
						WHEN  UPPER(EBI_IMPACT_CATEGORY) LIKE 'INT_DSC%'      THEN 'DISCOUNT - Internal'			    
						WHEN  UPPER(EBI_IMPACT_CATEGORY) = 'COMMIT'	          THEN 'DISCOUNT - Commit'
						WHEN  UPPER(EBI_IMPACT_CATEGORY) LIKE  '%VOL_DSC'	  THEN 'DISCOUNT - Volume'
						WHEN  UPPER(EBI_IMPACT_CATEGORY) IN ('START_DISC','MAN_ST_DSC')	THEN 'DISCOUNT - Startup'
						WHEN  (UPPER(EBI_IMPACT_CATEGORY) LIKE 'FREE_FULL_DISC%' OR upper(EBI_IMPACT_CATEGORY) like 'FREE_MAX_DISC%' )   THEN 'DISCOUNT - Free Trial'
						WHEN  UPPER(EBI_IMPACT_CATEGORY) = 'COMP_CYCLE'	      THEN 'Compute Cycle Benefit'
						WHEN  UPPER(EBI_IMPACT_CATEGORY) = 'BW_OUT'	          THEN 'BW_OUT Benefit'
						WHEN  UPPER(EBI_IMPACT_CATEGORY) = 'DISK_STOR'	      THEN 'Disk Storage Benefit'
						WHEN  UPPER(EBI_IMPACT_CATEGORY) = 'MSSQL_STOR'	      THEN 'MSSQL Storage Benefit'
						WHEN  UPPER(EBI_IMPACT_CATEGORY)  like 'SPL_DSC%'	  THEN 'DISCOUNT - Special'		
						WHEN  UPPER(EVENT_TYPE) LIKE '%_BWOUT'	  AND  UPPER(EBI_IMPACT_CATEGORY) like '%BW_OUT'	THEN concat('Bandwidth - Tier ' , SUBSTR(EBI_IMPACT_CATEGORY, 1, 1))
						WHEN  UPPER(EVENT_TYPE) LIKE '%_BWCDN'    AND (UPPER(EBI_IMPACT_CATEGORY) like '%BW_CDN%'	OR  UPPER(EBI_IMPACT_CATEGORY)  like  '%BW_ATARI%') THEN concat('DISCOUNT - Tier ',SUBSTR(EBI_IMPACT_CATEGORY, 1, 1))
						WHEN  UPPER(EVENT_TYPE) LIKE '%CDN_BWOUT' AND (UPPER(EBI_IMPACT_CATEGORY) like '%BW_CDN%'  OR   UPPER(EBI_IMPACT_CATEGORY)  like  '%CDBOUT') THEN concat('DISCOUNT - Tier ', SUBSTR(EBI_IMPACT_CATEGORY, 1, 1)) 	
				ELSE 'DISCOUNT'  
				END 
			  )
    WHEN  UPPER(EVENT_TYPE) = '/EVENT/BILLING/CYCLE/TAX'
    					THEN   (
						CASE TAX_TYPE_ID
							WHEN '0' THEN 'Federal Tax'
							WHEN '1' THEN 'State Tax'
							WHEN '2' THEN 'County Tax'
							WHEN '3' THEN 'Local Sales Tax'
							WHEN '8' THEN 'Local Sales Tax'
						ELSE 'Other Tax' END	  
					       )
    END AS Impact_Type_Description, --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Bill_Created_Date               --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
FROM    stage_one.raw_brm_dedicated_billing_daily_stage_master as DED 
);


-------------------------------------------------------------------------------------------------------
/*--- table build  ---*/


create or replace table stage_one.raw_ded_sessionlist as 
select distinct 
    EVENT_Session_obj_ido  AS session_obj_id0 
from 
    INV_DETAIL_A
where 
    Trx_Bill_Pay_Site_ID =0	;
	



UPDATE INV_DETAIL_A a
SET  
    Trx_Bill_Pay_Site_ID =service_poid
from  (
			SELECT DISTINCT --into 	#event_sess_user
				event_session_poid,
				service_poid,
				service_type,
				service_login,
				user_acct_poid,
				user_acct_brm_no,
				user_acct_gl_seg,
				user_acct_status,
				canon_company,
				concat(FIRST_NAME , ' ' , LAST_NAME) AS acct_USER_NAME
			FROM (
			SELECT
				e_sess.POID_ID0			  as event_session_poid,
				s.poid_id0				  as service_poid,
				s.poid_type			  as service_type,
				s.login				  as service_login,
				USER_ACCOUNT.poid_id0	  as user_acct_poid,
				USER_ACCOUNT.account_no	  as user_acct_brm_no,
				USER_ACCOUNT.gl_segment	  as user_acct_gl_seg,
				USER_ACCOUNT.status		  as user_acct_status,
				username.canon_company,
				FIRST_NAME,
				LAST_NAME	
			FROM
				`rax-landing-qa`.brm_ods.event_t AS e_sess 
			inner join 
				stage_one.raw_ded_sessionlist  l   -- limit ids for list pull
			on e_sess.poid_id0 = l.session_obj_id0
			INNER JOIN 
				`rax-landing-qa`.brm_ods.service_t AS S 
			ON e_sess.SERVICE_OBJ_ID0 = S.POID_ID0
			INNER JOIN 
				`rax-landing-qa`.brm_ods.account_t AS USER_ACCOUNT 
			ON S.ACCOUNT_OBJ_ID0 = USER_ACCOUNT.POID_ID0
			INNER JOIN
				`rax-landing-qa`.brm_ods.account_nameinfo_t AS username 
			ON USER_ACCOUNT.POID_ID0 = username.OBJ_ID0
			WHERE 
				lower(e_sess.POID_TYPE) = '/event/session' 
			)
) usr--#event_sess_user usr
where a.EVENT_Session_obj_ido = usr.event_session_poid
AND  a.Trx_Bill_Pay_Site_ID =0
AND(
	 service_poid IS NOT NULL
AND service_poid > 10000000);


-------------------------------------------------------------------------------------------------------------------
-- GET GL DETAILS
create or replace table   stage_one.raw_dedicated_inv_event_detail  as
select
master_unique_id
,Trx_Line_NK
,Trx_ID
,Trx_Line_ID
,Trx_Line_GL_Dist_ID
,Trx_Resource_ID
,Trx_Number
,Trx_Complete_Flag
,Trx_Bill_Pay_Site_ID
,Invoice_WKST_SNID
,Trx_Unit_Of_Measure_Code
,Trx_Raw_Start_Date
,Trx_Qty
,Trx_Term
,Trx_Description
,Account
,Server
,Organization
,Oracle_Department_ID
,Dedicated_Oracle_Department_ID
,Oracle_Department
,case when 
	(
		(lower(Oracle_Product_ID) in ('tric','tc_b','aws_'))
	or	(lower(Oracle_Product_ID) like '%data%' )
	or	(glsub7_product<>Oracle_Product_ID)
	)
	then glsub7_product
	else Oracle_Product_ID
 end as Oracle_Product_ID 
,Dedicated_Oracle_Product_ID
,Oracle_Product
,Oracle_Business_Unit_ID
,Dedicated_Business_Unit_ID
,Oracle_Business_Unit
,Oracle_Team_ID
,Dedicated_Oracle_Team_ID
,Oracle_Team
,Oracle_Company_ID
,Dedicated_Oracle_Company_ID
,Oracle_Company
,Oracle_Location_ID
,Dedicated_Oracle_Location_ID
,Oracle_Location
,GL_Account
,Oracle_GL_Account_DESC
,DED_usage_type
,Dedicated_Product_Group
,Dedicated_Product_Type
,product_poid
,product_type
,product_descr
,product_name
,product_code
,prod_permitted
,product_type2
,Invoice_Date
,Time_Month_Key
,TOTAL
,Extended_Amount
,Billing_Days_In_Month
,Currency_ID
,currency_abbrev
,currency_name
,Refresh_Date
,print_group
,Trx_Date
,Unit_Selling_Price
,event_poid
,EVENT_Item_Obj_Id0
,event_mod_dtt
,ebi_rec_id
,event_type
,item_type
,item_poid
,ITEM_NO
,ITEM_name
,Item_Effective_Date
,ITEM_Bill_Obj_Id0
,service_obj_type
,service_obj_id0
,gl_id
,session_obj_id0
,case when
		(
		 lower(Trx_Description) ='[sales tax only] incorrect tax rate'
		  AND Misc_Event_Billing_Type_Reason_ID=97
		  AND Misc_Event_Billing_Type IS NULL
		)
	then 'Sales Tax Only'
	else Misc_Event_Billing_Type
 end as 	Misc_Event_Billing_Type
,MISC_EVENT_BILLING_Type_Reason_ID
,MISC_EVENT_BILLING_Type_record_id
,case when
		(
		 lower(Trx_Description) ='[sales tax only] incorrect tax rate'
		  AND Misc_Event_Billing_Type_Reason_ID=97
		  AND Misc_Event_Billing_Type IS NULL
		)
	then 'Reason Codes-Debit Reasons'
	else String_Domain
 end as String_Domain
,case when
		(
		 lower(Trx_Description) ='[sales tax only] incorrect tax rate'
		  AND Misc_Event_Billing_Type_Reason_ID=97
		  AND Misc_Event_Billing_Type IS NULL
		)
	then 1
	else String_Version
 end as 	String_Version
,service_login_site_id
,ded_login_siteid
,BRM_ACCOUNT_NO
,account_poid
,Payment_Term
,Payment_Term_DESC
,GL_SEGMENT
,BUSINESS_TYPE
,BUSINESS_TYPE_DESCR
,RECORD_ID
,REGION
,Bill_NO
,ded_bill_end
,ded_bill_start
,BILL_START_DATE
,BILL_END_DATE
,BILL_MOD_DATE
,ded_prepay_start
,ded_prepay_end
,profile_bu
,case when
		(
		 lower(Trx_Description) ='[sales tax only] incorrect tax rate'
		  AND Misc_Event_Billing_Type_Reason_ID=97
		  AND Misc_Event_Billing_Type IS NULL
		)
	then 35238450545
	else Glid_Obj_Id0
 end as 	Glid_Obj_Id0
, case when
		(
		 lower(Trx_Description) ='[sales tax only] incorrect tax rate'
		  AND Misc_Event_Billing_Type_Reason_ID=97
		  AND Misc_Event_Billing_Type IS NULL
		)
	then 35238452593
	else GLSeg_Poid
 end as 	GLSeg_Poid
,case when
		(
		 lower(Trx_Description) ='[sales tax only] incorrect tax rate'
		  AND Misc_Event_Billing_Type_Reason_ID=97
		  AND Misc_Event_Billing_Type IS NULL
		)
	then 'ADJ Sales Tax Only'
	else Glid_Descr
 end as 	Glid_Descr 
,glid_rec_id
,GLAcct_rec_id
,GLAcct_attribute
,Attribute_Descr
,GLAcct_record_type
,Report_Type_Name
,GLAcct_offset_acct
,GLAcct_ar_acct
,glsub1_company
,glsub2_locationdc
,glsub3_acct_subprod
,glsub4_team
,glsub5_busunit
,glsub6_dept
,glsub7_product
,master_tbl_loaddtt
,LINE_OF_BUSINESS
,Event_Create_Date
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
,IMPACT_CATEGORY
,IMPACT_TYPE
,EVENT_earned_start_dtt
,EVENT_earned_end_dtt
,EVENT_FASTLANE_IS_BACKBILL
,FASTLANE_INV_DEAL_CODE
,FASTLANE_INV_GRP_CODE
,FASTLANE_INV_SUB_GRP_CODE
,FASTLANE_INV_Is_Backbill
,EBI_OFFERING_OBJ_ID0
,CASE 
	WHEN     IFNULL(Is_Backbill,0)=0 AND
			(
				( (CAST(EVENT_earned_end_dtt as date) <> '1970-01-01') AND IFNULL(EVENT_earned_end_dtt,'1970-01-01') <= IFNULL(Bill_Start_Date,'1970-01-01') )
			OR 	( IFNULL(FASTLANE_INV_Is_Backbill,0)<>0)
			OR	( IFNULL(EVENT_FASTLANE_IS_BACKBILL,0)<>0)
			OR	( IFNULL(LOWER(FASTLANE_INV_SUB_GRP_CODE),'unknown') like  '%backbill%')
			)
		THEN 1
	ELSE 0
 END AS Is_Backbill
,Event_Start_Dt
,Event_End_Date
,ACTIVITY_ATTR4
,ACTIVITY_ATTR5
,ACTIVITY_ATTR6
,EBI_rate_tag
,Tax_Type_Id
,Tax_Element_Id
,RATE
,Impact_Type_Description
,Bill_Created_Date
from(
SELECT
    ded.master_unique_id,
    CAST('0' AS STRING) AS Trx_Line_NK,
    ded.Trx_ID,
    ded.Trx_Line_ID,
    ded.Trx_Line_GL_Dist_ID,
    ded.Trx_Resource_ID,
    IFNULL(ded.Trx_Number, 'Unknown') AS Trx_Number,
    ded.Trx_Complete_Flag,
    ded.Trx_Bill_Pay_Site_ID,
    ded.Invoice_WKST_SNID,
    ded.Trx_Unit_Of_Measure_Code,
    --Trx_Is_Device_Account
    ded.Trx_Raw_Start_Date,
    ded.Trx_Qty,
    ded.Trx_Term,
    ded.Trx_Description,
    ded.Account,
    ded.Server,
    ded.Organization,
    ----------
    --- set IDs to default when not found
    coalesce(glsub6_dept,Oracle_Department_ID,'0000') AS Oracle_Department_ID,/* not in gl segs-all0000; not in brm ---- set to default when not found*/ 
    Oracle_Department_ID AS Dedicated_Oracle_Department_ID,
    ded.Oracle_Department,-- default unknown
    coalesce(GL.glsub7_product,ded.Oracle_Product_ID,'0000') AS Oracle_Product_ID,
    Oracle_Product_ID AS Dedicated_Oracle_Product_ID,
    ded.Oracle_Product,-- default unknown
    coalesce(glsub5_busunit,Oracle_Business_Unit_ID,'0000') AS Oracle_Business_Unit_ID, /* not in gl segs-all0000; not in brm*/ 
    Oracle_Business_Unit_ID AS Dedicated_Business_Unit_ID,
    ded.Oracle_Business_Unit,-- default unknown
    coalesce(ded.Oracle_Team_ID,GL.glsub4_team,'000') AS Oracle_Team_ID,/* not in gl segs-all000*/
    Oracle_Team_ID AS Dedicated_Oracle_Team_ID,
    ded.Oracle_Team,-- from ded; default null
    coalesce(ded.Oracle_Company_ID,GL.glsub1_company,'000') AS Oracle_Company_ID,/* not in gl segs-all000*/
    Oracle_Company_ID AS Dedicated_Oracle_Company_ID,
    ded.Oracle_Company,-- from ded; default null
    coalesce( glsub2_locationdc,Oracle_Location_ID ,'000') AS Oracle_Location_ID,    -- from gl segs
    Oracle_Location_ID AS Dedicated_Oracle_Location_ID,
    ded.Oracle_Location, -- partial from ded; default null
    -----------
    IFNULL(IFNULL(GL.glsub3_acct_subprod,ded.GL_Account),	'000000') AS GL_Account,  --partial ded; else gl segs; default null
    Oracle_GL_Account_DESC,
    ded.DED_usage_type,
    Dedicated_Product_Group,
    Dedicated_Product_Type,
    product_poid,	
    product_type,	
    product_descr,	
    product_name,	
    product_code,	
    prod_permitted,
    product_type2,
    ded.Invoice_Date,
    Time_Month_Key,
    ded.TOTAL,
    ded.Extended_Amount,
    ded.Billing_Days_In_Month,
    ded.Currency_ID,
    currency_abbrev,
    currency_name,	
    ded.Refresh_Date,
    ded.print_group,
    ded.Trx_Date,
    round(CAST(ded.Unit_Selling_Price as numeric),2) AS Unit_Selling_Price,
    ------------------------------------------
    EVENT_POID_ID0 AS event_poid,
    EVENT_Item_Obj_Id0,
    DED.event_mod_dtt,
    ded.ebi_rec_id,
    ded.event_type,
    ded.item_type,
    ITEM_POID_ID0 AS item_poid,
    ded.ITEM_NO,
    ded.ITEM_name,
    Item_Effective_Date,
    ITEM_Bill_Obj_Id0,
    EVENT_service_obj_type AS service_obj_type,
    EVENT_service_obj_id0 AS service_obj_id0,
    EBI_GL_ID AS gl_id,
    EVENT_service_obj_id0 AS session_obj_id0,
    MISC_EVENT_BILLING_Type,			---Credit_Type
    MISC_EVENT_BILLING_Type_Reason_ID,  --Credit_Reason_ID
    MISC_EVENT_BILLING_Type_record_id,
    string_domain,
    string_version,     
    ded.service_login_site_id,
    ded.ded_login_siteid,
    ded.BRM_ACCOUNT_NO,
    ded.account_poid,
    PAYMENT_TERM AS Payment_Term,
    CAST('Unknown' As STRING) AS Payment_Term_DESC,
    ded.GL_SEGMENT,
    ded.BUSINESS_TYPE,
    ded.BUSINESS_TYPE_DESCR,
    DED_record_id AS RECORD_ID,
    DED_region AS REGION,
    Bill_NO,
    ded.ded_bill_end,   -- used for pre-pays
    ded.ded_bill_start, -- used for pre-pays
    ded.BILL_START_DATE, -- used for pre-pays
    ded.BILL_END_DATE, -- used for pre-pays
    ded.BILL_MOD_DATE,
    ded_prepay_start,  -- new field coming to event dedicated data table_no target date yet_11.14.16jcm
    ded_prepay_end,  -- new field coming to event dedicated data table_no target date yet_11.14.16jcm
    ded.profile_bu,
    ---------------------------------------------
    GL.glid_obj_id0,
    GL.GLSeg_poid,
    GL.glid_descr,
    GL.glid_rec_id, -- same as GL.GLAcct_rec_id2,
    GL.GLAcct_rec_id,
    GL.GLAcct_attribute,
    atr.Attribute_Descr,
    GL.GLAcct_record_type,
    typ.Report_Type_Name,
    GL.GLAcct_offset_acct,
    GL.GLAcct_ar_acct,
    glsub1_company,
    glsub2_locationdc,
    glsub3_acct_subprod,
    glsub4_team,
    glsub5_busunit,
    glsub6_dept,
    glsub7_product,
    ded.master_tbl_loaddtt,
    LINE_OF_BUSINESS,
    Event_Create_Date,
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
    IMPACT_CATEGORY,
    IMPACT_TYPE,
    EVENT_earned_start_dtt, 
    EVENT_earned_end_dtt,
    IFNULL(EVENT_FASTLANE_IS_BACKBILL,0) AS EVENT_FASTLANE_IS_BACKBILL,
    FASTLANE_INV_DEAL_CODE,
    FASTLANE_INV_GRP_CODE,
    FASTLANE_INV_SUB_GRP_CODE,
    IFNULL(FASTLANE_INV_Is_Backbill,0) AS FASTLANE_INV_Is_Backbill,
    EBI_OFFERING_OBJ_ID0,
    0 AS Is_Backbill,
	Event_Start_dtt AS Event_Start_Dt,
	EVENT_end_dtt    AS Event_End_Date,
	ACTIVITY_ATTR4, -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR5, -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR6,	-- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	EBI_rate_tag, ---- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Tax_Element_Id,      --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	RATE,				--new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Impact_Type_Description,  --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Bill_Created_Date         --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging

FROM INV_DETAIL_A ded
left outer join
    stage_one.raw_brm_glid_account_config GL 
    ON ded.EBI_GL_ID= glid_rec_id
inner join 
    stage_two_dw.stage_brm_glid_acct_atttribute atr
on GL.GLAcct_attribute  = atr.Attribute_id
inner join 
    stage_two_dw.stage_brm_glid_acct_report_type typ
ON GL.GLAcct_record_type=typ.Report_Type_id

AND ded.GL_SEGMENT= GL.GLSeg_name 
AND GL.GLAcct_record_type In (2,8) -- gl for BILLED events only
AND GL.GLAcct_attribute= 1  -- gl for net amounts only
);

------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------
/** BRM GL SEGMENT DETAILS  **/
/**  Oracle GL Values  ***/

create or replace temp table flex_values as
SELECT --INTO	#flex_values
	flex_value,
	DESCRIPTION,
	flex_value_set_name
FROM (
SELECT DISTINCT
	flex_value,
	ffvt.DESCRIPTION,
	flex_value_set_name
FROM
	`rax-landing-qa`.operational_reporting_oracle.raw_fnd_flex_values    ffv  
LEFT OUTER JOIN
	`rax-landing-qa`.operational_reporting_oracle.raw_fnd_flex_value_sets   ffvs  
ON ffv.flex_value_set_id = ffvs.flex_value_set_id
LEFT OUTER JOIN
	`rax-landing-qa`.operational_reporting_oracle.raw_fnd_flex_values_tl     ffvt   
ON ffv.flex_value_id = ffvt.flex_value_id	
WHERE
	upper(ffvs.flex_value_set_name) IN('RS_TEAM','RS_BUSINESS_UNITS','RS_DEPARTMENTS','RS_COMPANY', 'RS_PRODUCT','RS_LOCATION', 'RS_ACCOUNT') 
	);


	
UPDATE   stage_one.raw_dedicated_inv_event_detail ded
SET 
    Oracle_Department = seg.DESCRIPTION
FROM flex_values seg
where ded.Oracle_Department_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_DEPARTMENTS'
;
--print ('dept updated')
-----------------
--PRODUCT
------------------
UPDATE  stage_one.raw_dedicated_inv_event_detail ded
SET  
    Oracle_Product = seg.DESCRIPTION
FROM flex_values seg
where ded.Oracle_Product_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_PRODUCT'
AND upper(ded.Oracle_Product) = 'UNKNOWN'
;

--9891 updated
--print ('prod updated')
------------------------
--BUSINESS UNIT
------------------------
UPDATE  stage_one.raw_dedicated_inv_event_detail ded
SET  
    Oracle_Business_Unit = seg.DESCRIPTION
FROM flex_values seg
where ded.Oracle_Business_Unit_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_BUSINESS_UNITS'
AND upper(ded.Oracle_Business_Unit) = 'UNKNOWN';
--print ('BUsUnit updated')
------------------------
--TEAM
------------------------
UPDATE   stage_one.raw_dedicated_inv_event_detail ded
SET     Oracle_Team = seg.DESCRIPTION
FROM flex_values seg
where ded.Oracle_Team_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_TEAM'
AND upper(ded.Oracle_Team)  = 'UNKNOWN'
;
--print ('team updated')
------------------------
--COMPANY
------------------------
UPDATE  stage_one.raw_dedicated_inv_event_detail ded
SET  
    Oracle_Company = seg.DESCRIPTION
FROM flex_values seg
WHERE ded.Oracle_Company_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_COMPANY'
AND ded.Oracle_Company IS NULL
;
--print ('comp updated')
------------------------
--LOCATION
------------------------
UPDATE stage_one.raw_dedicated_inv_event_detail ded 
SET  
    Oracle_Location = seg.DESCRIPTION
FROM flex_values seg
WHERE ded.Oracle_Location_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_LOCATION'
AND ded.Oracle_Location IS NULL;
--print ('location updated')
------------------------
--GL Account Desc
------------------------
UPDATE  stage_one.raw_dedicated_inv_event_detail ded 
SET  
    Oracle_GL_Account_DESC = seg.DESCRIPTION
FROM flex_values seg
WHERE ded.GL_Account = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_ACCOUNT'
AND upper(ded.Oracle_GL_Account_DESC) ='UNKNOWN';
--print ('GL Account updated')
------------------------------------------------------
--GL Account for tax
UPDATE stage_one.raw_dedicated_inv_event_detail A
SET
    gl_Account='220800'
WHERE
   GL_ACCOUNT='000000' 
AND lower(event_type) Like '%tax%';
------------------------------------------------------
UPDATE stage_one.raw_dedicated_inv_event_detail A
SET
   Dedicated_Product_Group='Tax',
   Dedicated_Product_Type='Tax'
WHERE
    lower(event_type) Like '%tax%'
AND Dedicated_Product_Group IS NULL;
------------------------------------------------------
UPDATE stage_one.raw_dedicated_inv_event_detail A
SET   A.Payment_Term_DESC=B.Payment_Term_Desc
FROM 
   stage_two_dw.stage_payment_term  B
where A.Payment_Term=B.Payment_Term;
------------------------------------------------------
UPDATE stage_one.raw_dedicated_inv_event_detail A
SET
   LINE_OF_BUSINESS='DEDICATED'
WHERE
      lower(LINE_OF_BUSINESS)='dedicated';
	  

UPDATE stage_one.raw_dedicated_inv_event_detail A
SET 	
    Trx_Line_NK=cast(concat(CAST(master_unique_id as STRING),'--',CAST(gl_Account as STRING),'--',CAST(Time_Month_Key as STRING),'--',CAST(IFNULL(Server,'1') as STRING),'--',(case when Trx_Term = 0 then CAST(Trx_Qty AS STRING) else CAST(trx_term as STRING) end)) as string)
	where true;
	
	
-------------------------------------------------------------------------------------------------------------------
--SELECT DISTINCT Oracle_Team FROM  Dedicated_Inv_Event_Detail_Stageraw_dedicated_inv_event_detail
SELECT DISTINCT master_unique_id  --INTO    #Modified_Tickets
FROM  stage_one.raw_brm_dedicated_billing_daily_stage_master;

DELETE FROM 
	stage_two_dw.stage_dedicated_inv_event_detail  A
WHERE
   EXISTS
(
SELECT
	master_unique_id
FROM stage_one.raw_brm_dedicated_billing_daily_stage_master XX
WHERE
	XX.master_unique_id =A.master_unique_id
);
-------------------------------------------------------------------------------------------------------------------
INSERT INTO    stage_two_dw.stage_dedicated_inv_event_detail 
   (
 master_unique_id
,Trx_Line_NK
,Trx_ID
,Trx_Line_ID
,Trx_Line_GL_Dist_ID
,Trx_Resource_ID
,Trx_Number
,Trx_Complete_Flag
,Trx_Bill_Pay_Site_ID
,Invoice_WKST_Snid
,Trx_Unit_Of_Measure_Code
,Trx_Is_Device_Account
,Trx_Raw_Start_Date
,Trx_Qty
,Trx_Term
,Trx_Description
,Account
,Server
,Organization
,Oracle_Department_ID
,Oracle_Department
,Oracle_Product_ID
,Oracle_Product
,Oracle_Business_Unit_ID
,Oracle_Business_Unit
,Oracle_Team_ID
,Oracle_Team
,Oracle_Company_ID
,Oracle_Company
,Oracle_Location_ID
,Oracle_Location
,GL_Account
,Oracle_GL_Account_DESC
,DED_Usage_Type
,Dedicated_Product_Group
,Dedicated_Product_Type
,Product_Poid
,Product_Type
,Product_Descr
,Product_Name
,Product_Code
,Prod_Permitted
,Product_Type2
,Invoice_Date
,Time_Month_Key
,TOTAL
,Extended_Amount
,Billing_Days_In_Month
,Currency_ID
,Currency_Abbrev
,Currency_Name
,Refresh_Date
,Print_Group
,Trx_Date
,Unit_Selling_Price
,Event_Poid
,Event_Mod_Dtt
,Event_Type
,Misc_Event_Billing_Type
,Misc_Event_Billing_Type_Reason_ID
,Misc_Event_Billing_Type_Record_ID
,String_Domain
,String_Version
,Item_Type
,Item_Poid
,Item_No
,Item_Name
,Item_Effective_Date
,Service_Obj_Type
,Service_Obj_Id0
,Ebi_Rec_Id
,Gl_Id
,Session_Obj_Id0
,Service_Login_Site_Id
,Ded_Login_Siteid
,Brm_Account_No
,Account_Poid
,GL_Segment
,Business_Type
,Business_Type_DESCR
,Record_ID
,Region
,Payment_Term
,Payment_Term_DESC
,Bill_NO
,Ded_Bill_End
,Ded_Bill_Start
,Ded_Prepay_Start
,Ded_Prepay_End
,Bill_Start_Date
,Bill_End_Date
,Bill_Mod_Date
,Profile_Bu
,Glid_Obj_Id0
,GLSeg_Poid
,Glid_Descr
,Glid_Rec_Id
,GLAcct_Rec_ID
,GLAcct_Attribute
,Attribute_Descr
,GLAcct_Record_Type
,Report_Type_Name
,GLAcct_Offset_Acct
,GLAcct_Ar_Acct
,Glsub1_Company
,Glsub2_Locationdc
,Glsub3_Acct_Subprod
,Glsub4_Team
,Glsub5_Busunit
,Glsub6_Dept
,Glsub7_Product
,etldtt
,EVENT_Item_Obj_Id0
,ITEM_Bill_Obj_Id0
,LINE_OF_BUSINESS
,Event_Create_Date
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
,IMPACT_CATEGORY
,IMPACT_TYPE
,Event_Start_Dt
,EVENT_End_Date
,EVENT_earned_start_dtt
,EVENT_earned_end_dtt
,EVENT_FASTLANE_IS_BACKBILL
,FASTLANE_INV_DEAL_CODE
,FASTLANE_INV_GRP_CODE
,FASTLANE_INV_SUB_GRP_CODE
,FASTLANE_INV_Is_Backbill
,Is_Backbill
,EBI_OFFERING_OBJ_ID0
,Invoice_Nk
,Invoice_Source_field_NK
,Invoice_Attribute_NK
,Invoice_Attribute_Source_field_NK
,ACTIVITY_ATTR4 -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,ACTIVITY_ATTR5 -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,ACTIVITY_ATTR6	-- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,EBI_rate_tag ---- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,ACTIVITY_ATTR7 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging No mapping just hardcoded column
,ACTIVITY_ATTR8 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging No mapping just hardcoded column
,Tax_Type_Id		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
,Tax_Element_Id      --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
,RATE      			--new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
,Impact_Type_Description  --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
,Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
,Global_Account_Type
,Item_Tag
)
SELECT 
    master_unique_id,
    Trx_Line_NK,
    Trx_ID,
    Trx_Line_ID,
    Trx_Line_GL_Dist_ID,
    Trx_Resource_ID,
    IFNULL(Trx_Number,'Unknown')  AS Trx_Number,
    Trx_Complete_Flag,
    Trx_Bill_Pay_Site_ID,
    Invoice_WKST_SNID,
    Trx_Unit_Of_Measure_Code,
    (case 
	   when 
		  Product_type is not null 
	   THEN
		  case when upper(Product_type) ='HOSTING SERVICE' THEN 'DE' ELSE 'AC' END   -- no logic from sources yet on how to id dev vs acct_jcm_10.12.16
	   else 
		  'Unknown'  
	 END ) AS Trx_Is_Device_Account,  
    Trx_Raw_Start_Date,
    Trx_Qty,
    CAST(Trx_Term AS STRING) AS Trx_Term,
    Trx_Description,
    Account,
    Server,
    IFNULL(Organization,'Unknown') AS Organization,
    Oracle_Department_ID,	
	CASE 
		   WHEN oracle_department like '%)%' THEN 
		   LTRIM(RTRIM(REPLACE(substr(oracle_department, strpos(oracle_department, ')'), length(oracle_department)), ')', ' ')))
		ELSE
		   coalesce(oracle_department,'Unknown')	
	END AS oracle_department,
    Oracle_Product_ID,
	CASE 
		   WHEN oracle_product like '%)%' THEN 
		   LTRIM(RTRIM(REPLACE(substr(oracle_product, strpos( oracle_product, ')'), length(oracle_product)), ')', ' ')))
		ELSE
		   coalesce(oracle_product,'Unknown')	
	END AS oracle_product,
    Oracle_Business_Unit_ID,
	CASE 
	   WHEN oracle_business_unit like '%)%' THEN 
	   LTRIM(RTRIM(REPLACE(substr(oracle_business_unit, strpos(oracle_business_unit, ')'), length(oracle_business_unit)), ')', ' ')))
	ELSE
	   coalesce(oracle_business_unit,'Unknown')	
	END AS oracle_business_unit,
    Oracle_Team_ID,	
	CASE 
		WHEN oracle_team like '%)%' THEN 
		   LTRIM(RTRIM(REPLACE(substr(oracle_team, strpos(oracle_team,')'), length(oracle_team)), ')', ' ')))
		ELSE
		   coalesce(oracle_team,'Unknown')	
	END AS oracle_team,
    Oracle_Company_ID,
	CASE 
		WHEN oracle_company like '%)%' THEN 
		   LTRIM(RTRIM(REPLACE(substr(oracle_company, strpos(oracle_company , ')'), length(oracle_company)), ')', ' ')))
		ELSE
		   coalesce(oracle_company,'Unknown')	
		END AS oracle_company,
    Oracle_Location_ID,
	CASE 
		   WHEN oracle_location like '%)%' THEN 
		   LTRIM(RTRIM(REPLACE(substr(oracle_location, strpos(oracle_location , ')'), length(oracle_location)), ')', ' ')))
		ELSE
		   coalesce(oracle_location,'Unknown')	
	END AS oracle_location,
    IFNULL(GL_Account,'000000') AS GL_Account,
	CASE 
		   WHEN oracle_gl_account_desc like '%)%' THEN 
		   LTRIM(RTRIM(REPLACE(substr(oracle_gl_account_desc, strpos(oracle_gl_account_desc , ')'), length(oracle_gl_account_desc)), ')', ' ')))
		ELSE
		   coalesce(oracle_gl_account_desc,'Unknown')	
	END AS oracle_gl_account_desc,
    DED_usage_type,
    Dedicated_Product_Group,
    Dedicated_Product_Type,
    Product_Poid,
    Product_Type,
    Product_Descr,
    Product_Name,
    Product_Code,
    Prod_Permitted,
    Product_Type2,
    Invoice_Date,
    Time_Month_Key,
    CAST(TOTAL AS NUMERIC ) AS TOTAL,
    CAST(Extended_Amount AS NUMERIC ) AS Extended_Amount,
    Billing_Days_In_Month,
    CAST(Currency_ID AS STRING) AS Currency_ID,
    Currency_abbrev,
    Currency_name,
    Refresh_Date,
    print_group,
    Trx_Date,
    Unit_Selling_Price,
    event_poid,
    event_mod_dtt,
    event_type,
    MISC_EVENT_BILLING_Type,			---Credit_Type
    MISC_EVENT_BILLING_Type_Reason_ID,  --Credit_Reason_ID
    MISC_EVENT_BILLING_Type_record_id,
    string_domain,
    string_version,  
    item_type,
    item_poid,		
    ITEM_NO,
    ITEM_Name, 
    CAST(Item_Effective_Date AS STRING) AS Item_Effective_Date,
    service_obj_type,
    service_obj_id0,
    Ebi_Rec_Id,
    gl_id,
    session_obj_id0,       
    service_login_site_id,
    ded_login_siteid,
    BRM_ACCOUNT_NO,
    account_poid,
    GL_SEGMENT,
    CAST(BUSINESS_TYPE AS STRING) AS BUSINESS_TYPE ,
    BUSINESS_TYPE_DESCR,
    RECORD_ID,
    REGION,
    CAST(Payment_Term AS INT64) AS Payment_Term,
    Payment_Term_DESC,
    Bill_NO,
    IFNULL(ded_bill_end,'1900-01-01') AS ded_bill_end,			 -- used for pre-pays
    IFNULL(ded_bill_start,'1900-01-01') AS ded_bill_start,			  -- used for pre-pays
    IFNULL(ded_prepay_start,'1900-01-01') AS ded_prepay_start,	  -- new field coming to event dedicated data table_no target date yet_11.14.16jcm
    IFNULL(ded_prepay_end,'1900-01-01') AS ded_prepay_end, -- new field coming to event dedicated data table_no target date yet_11.14.16jcm
    IFNULL(BILL_START_DATE,'1900-01-01') AS BILL_START_DATE,		 -- used for pre-pays
    IFNULL(BILL_END_DATE,'1900-01-01') AS BILL_END_DATE,		  -- used for pre-pays
    IFNULL(BILL_MOD_DATE,'1900-01-01') AS BILL_MOD_DATE,
    profile_bu,
    glid_obj_id0,
    GLSeg_poid,
    glid_descr,
    glid_rec_id,
    GLAcct_rec_id,
    GLAcct_attribute,
    Attribute_Descr,
    GLAcct_record_type,
    Report_Type_Name,
    GLAcct_offset_acct,
    GLAcct_ar_acct,
    glsub1_company,
    glsub2_locationdc,
    glsub3_acct_subprod,
    glsub4_team,
    glsub5_busunit,
    glsub6_dept,
    glsub7_product,    
    current_datetime()					AS etldtt,
    EVENT_Item_Obj_Id0,
    ITEM_Bill_Obj_Id0,
	LINE_OF_BUSINESS,
	Event_Create_Date,
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
    IMPACT_CATEGORY,
    CAST(IMPACT_TYPE AS STRING) AS IMPACT_TYPE,
	Event_Start_Dt,
	Event_End_Date,
    EVENT_earned_start_dtt, 
    EVENT_earned_end_dtt,
    EVENT_FASTLANE_IS_BACKBILL,
    FASTLANE_INV_DEAL_CODE,
    FASTLANE_INV_GRP_CODE,
    FASTLANE_INV_SUB_GRP_CODE,
    FASTLANE_INV_Is_Backbill,
    Is_Backbill,
	EBI_OFFERING_OBJ_ID0,
	TO_BASE64(MD5( CONCAT( BILL_NO, '|',
	         ACCOUNT_POID,'|',
			Trx_Unit_Of_Measure_Code,'|',
			impact_category ,'|',
			IMPACT_TYPE,'|',
			Trx_Description ,'|',
			Service_Obj_Type,'|'     
			) ) ) AS Invoice_Nk 
 ,'Bill_Number|Billing_Application_Account_Number|Unit_Measure_Of_Code|Impact_Category|Impact_Type_Id|Impact_Type_Description|Service_Type' Invoice_Source_field_NK    
,TO_BASE64(MD5( CONCAT(
  CAST(COALESCE(EVENT_FASTLANE_SERVICE_TYPE,FASTLANE_INV_GRP_CODE,'N/A') AS STRING ) ,'|'
, CAST( COALESCE(EVENT_FASTLANE_EVENT_TYPE,FASTLANE_INV_SUB_GRP_CODE,'N/A')  AS STRING ) ,'|'
, COALESCE(CAST( EVENT_FASTLANE_DC_ID AS STRING) ,'N/A') ,'|'
, COALESCE(CAST( EVENT_FASTLANE_REGION AS STRING) ,'N/A') ,'|'
, COALESCE(CAST( EVENT_FASTLANE_RESOURCE_ID AS STRING) ,'N/A'),'|'
, COALESCE(CAST( EVENT_FASTLANE_RESOURCE_NAME AS STRING),'N/A'),'|'
, COALESCE(CAST(EVENT_FASTLANE_ATTR1 AS STRING ) ,'N/A') ,'|'
, COALESCE(CAST(EVENT_FASTLANE_ATTR2 AS STRING),'N/A'),'|'
, COALESCE(CAST(EVENT_FASTLANE_ATTR3 AS STRING),'N/A')    ,'|'
, COALESCE(CAST(ACTIVITY_ATTR4 AS STRING),'N/A')             ,'|'
, COALESCE(CAST(ACTIVITY_ATTR5 AS STRING),'N/A')                 ,'|'
, COALESCE(CAST(ACTIVITY_ATTR6 AS STRING),'N/A')                     ,'|'
, 'N/A'
, 'N/A'
, COALESCE(CAST(FASTLANE_IMPACT_CATEGORY AS STRING),'N/A'),'|'
, COALESCE(CAST(FASTLANE_IMPACT_VALUE AS STRING),'N/A')       ,'|'
, COALESCE(CAST(FASTLANE_INV_DEAL_CODE AS STRING),'N/A')        ,'|'
--,  COALESCE(EVENT_FASTLANE_RECORD_ID,Record_Id,'N/A')   ,'|'
, COALESCE (CAST(RATE AS STRING),'N/A')  ,'|'
, COALESCE(CAST(EBI_rate_tag AS STRING),'N/A')    ,'|'
, COALESCE(CAST(Tax_Type_Id AS STRING),'N/A')    ,'|'
, COALESCE(CAST(TAX_ELEMENT_ID AS STRING),'N/A')   ,'|'					  
) ) ) AS Invoice_Attribute_NK
 , 'Invoice_Group_Code|Invoice_Sub_Group_Code|Fastlane_Data_Center_Id|Fastlane_Region|Invoice_Resource_Id|Invoice_Resource_Name|Fastlane_Attr1|Fastlane_Attr2|Fastlane_Attr3|Fastlane_Attr4|Fastlane_Attr5|Fastlane_Attr6|Fastlane_Attr7|Fastlane_Attr8|Fastlane_Impact_Category|Fastlane_Impact_Value|Deal_Code|Rate|Rate_Tag|Tax_Type_Id|Tax_Element_Id' Invoice_Attribute_Source_field_NK 
 ,ACTIVITY_ATTR4 -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,ACTIVITY_ATTR5 -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,ACTIVITY_ATTR6	-- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,EBI_rate_tag ---- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
,'N/A' as ACTIVITY_ATTR7 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging No mapping just hardcoded column
,'N/A' as ACTIVITY_ATTR8 --new field added 7.10.2019 anil2912 as per uday request for NRD Staging No mapping just hardcoded column
,Tax_Type_Id		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
,Tax_Element_Id      --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
,RATE				--new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
,Impact_Type_Description  --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
, DATEtime(TIMESTAMP_SECONDS(CAST(Bill_Created_Date AS INT64) ))   As Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
,case when LOWER(ded.line_of_business)='dedicated' then 'Managed_Hosting'
	when LOWER(ded.line_of_business)='datapipe' then 'Datapipe'
	when LOWER(ded.line_of_business) like '%cloud%' then 'Cloud'
	when LOWER(ded.line_of_business) like '%email%' then 'Mailtrust'
		else '' 
 end AS Global_Account_Type,
 CASE 
	WHEN LOWER(ded.Item_Type) = '/item/adjustment' 			THEN 'Adjustment'
	WHEN LOWER(ded.Item_Type) = '/item/payment' 			THEN 'Payment'
	WHEN LOWER(ded.Item_Type) = '/item/payment/reversal' 	THEN 'Reversal'
	WHEN LOWER(ded.Item_Type) = '/item/purchase' 			THEN 'Purchase'
	WHEN LOWER(ded.Item_Type) = '/item/cycle_tax' 			THEN 'Tax'
	ELSE NULL 
END AS Item_Tag
FROM 
    stage_one.raw_dedicated_inv_event_detail DED;
	
---------------------------------------------------------------------------------------------------
UPDATE  stage_two_dw.stage_dedicated_inv_event_detail 
SET 
    glid_obj_id0	=	GL.glid_obj_id0,
    GLSeg_poid	=	    GL.GLSeg_poid,
    glid_descr	=	    GL.glid_descr,
    glid_rec_id	=	    GL.glid_rec_id, 
    GLAcct_rec_id	=	    GL.GLAcct_rec_id,
    GLAcct_attribute	=	    GL.GLAcct_attribute,
    Attribute_Descr	=	    atr.Attribute_Descr,
    GLAcct_record_type	=	    IFNULL(GL.GLAcct_record_type,8),
    Report_Type_Name	=	    typ.Report_Type_Name,
    GLAcct_offset_acct	=	    GL.GLAcct_offset_acct,
    GLAcct_ar_acct	=	    GL.GLAcct_ar_acct,
    glsub1_company	=	   IFNULL(GL.glsub1_company, '000'),
    glsub2_locationdc	=	   IFNULL(GL.glsub2_locationdc, '000'),
    glsub3_acct_subprod=	IFNULL(GL.glsub3_acct_subprod, '000000'),
    glsub4_team	=	   IFNULL(GL.glsub4_team, '000'),
    glsub5_busunit	=	 IFNULL(GL.glsub5_busunit, '0000'),
    glsub6_dept	=	    IFNULL(GL.glsub6_dept, '0000'),
    glsub7_product	=	    IFNULL(GL.glsub7_product, '0000')
FROM
   stage_two_dw.stage_dedicated_inv_event_detail A
INNER JOIN
    stage_one.raw_brm_glid_account_config GL 
    ON A.gl_id= Gl.glid_rec_id
AND A.GL_SEGMENT= GL.GLSeg_name 
inner join 
     stage_two_dw.stage_brm_glid_acct_atttribute atr
on GL.GLAcct_attribute  = atr.Attribute_id
inner join 
     stage_two_dw.stage_brm_glid_acct_report_type typ
ON GL.GLAcct_record_type=typ.Report_Type_id

AND GL.GLAcct_record_type IN (2,8) -- gl for BILLED events only
AND GL.GLAcct_attribute= 1  -- gl for net amounts only
WHERE
   ( IFNULL(A.glsub7_product, '0000')<>IFNULL(GL.glsub7_product, '0000')
   OR IFNULL(A.glsub3_acct_subprod, '000000')<>IFNULL(GL.glsub3_acct_subprod, '000000')
   OR IFNULL(A.GLAcct_record_type, 8)<>IFNULL(GL.GLAcct_record_type, 8)
   OR IFNULL(A.glsub1_company, '000')<>IFNULL(GL.glsub1_company, '000')
   OR IFNULL(A.glsub2_locationdc, '000')<> IFNULL(GL.glsub2_locationdc, '000')
   OR IFNULL(A.glsub4_team, '000')<> IFNULL(GL.glsub4_team, '000')
   OR IFNULL(A.glsub5_busunit, '0000')<>IFNULL(GL.glsub5_busunit, '0000')
   OR IFNULL(A.glsub6_dept, '0000')<> IFNULL(GL.glsub6_dept, '0000'));
---------------------------------------------------------------------------------------------------

UPDATE stage_two_dw.stage_dedicated_inv_event_detail A
SET
    Oracle_Product_ID=IFNULL(glsub7_product,'0000') 
WHERE    IFNULL(glsub7_product,'0000') <>IFNULL(Oracle_Product_ID ,'0000');
---------------------------------------------------------------------------------------------------
UPDATE  stage_two_dw.stage_dedicated_inv_event_detail A
SET  GL_Account=IFNULL(glsub3_acct_subprod,'000000')
WHERE 	IFNULL(glsub3_acct_subprod,'000000') <>IFNULL(GL_Account ,'000000');
---------------------------------------------------------------------------------------------------
UPDATE stage_two_dw.stage_dedicated_inv_event_detail A
SET
    Oracle_Department_ID='0000'--IFNULL(glsub6_dept,'0000')
WHERE	IFNULL(glsub6_dept,'0000')<>IFNULL(Oracle_Department_ID ,'0000');
---------------------------------------------------------------------------------------------------
UPDATE stage_two_dw.stage_dedicated_inv_event_detail  A
SET Oracle_Business_Unit_ID='0000' --IFNULL(glsub5_busunit,'0000')
WHERE	IFNULL(glsub5_busunit,'0000') <>IFNULL(Oracle_Business_Unit_ID ,'0000');
---------------------------------------------------------------------------------------------------
UPDATE stage_two_dw.stage_dedicated_inv_event_detail  A
SET
   Oracle_Location_ID=IFNULL(glsub2_locationdc,'000')
WHERE	IFNULL(glsub2_locationdc,'000') <>IFNULL(Oracle_Location_ID ,'000');
------------------------------------------------------------------------------------------------------------
--BU
------------------------
UPDATE  stage_two_dw.stage_dedicated_inv_event_detail ded
SET  
    Oracle_Business_Unit = IFNULL(seg.DESCRIPTION, 'Unknown')
FROM flex_values seg
WHERE ded.Oracle_Business_Unit_ID = seg.flex_value
AND  UPPER(seg.flex_value_set_name)='RS_BUSINESS_UNITS'
AND  UPPER(IFNULL(ded.Oracle_Business_Unit, 'Unknown')) <>   UPPER(IFNULL(seg.DESCRIPTION, 'Unknown'));
------------------------
--TEAM
------------------------
UPDATE  stage_two_dw.stage_dedicated_inv_event_detail  ded
SET  
    Oracle_Team =  IFNULL(seg.DESCRIPTION, 'Unknown')
FROM flex_values seg
WHERE ded.Oracle_Team_ID = seg.flex_value
AND UPPER(seg.flex_value_set_name)='RS_TEAM'
AND UPPER(IFNULL(Oracle_Team, 'Unknown')) = UPPER(IFNULL(seg.DESCRIPTION, 'Unknown'));
--print ('team updated')
------------------------
--COMPANY
------------------------
UPDATE   stage_two_dw.stage_dedicated_inv_event_detail ded
SET  
    Oracle_Company =  IFNULL(seg.DESCRIPTION, 'Unknown')
FROM flex_values seg
WHERE ded.Oracle_Company_ID = seg.flex_value
AND UPPER(seg.flex_value_set_name)='RS_COMPANY'
AND UPPER(IFNULL(Oracle_Company, 'Unknown')) = UPPER(IFNULL(seg.DESCRIPTION, 'Unknown'));

--print ('comp updated')
------------------------
--LOCATION
------------------------
UPDATE  stage_two_dw.stage_dedicated_inv_event_detail ded
SET  
    Oracle_Location =  IFNULL(seg.DESCRIPTION, 'Unknown')
FROM flex_values seg
WHERE ded.Oracle_Location_ID = seg.flex_value
AND UPPER(seg.flex_value_set_name)='RS_LOCATION'
AND UPPER(IFNULL(Oracle_Location, 'Unknown')) = UPPER(IFNULL(seg.DESCRIPTION, 'Unknown'));
--print ('location updated')
------------------------
--Product
------------------------
UPDATE  stage_two_dw.stage_dedicated_inv_event_detail A
SET  
    Oracle_Product = IFNULL(seg.GL_Product_Description,'Unknown')
FROM stage_two_dw.stage_gl_products seg
WHERE A.Oracle_Product_ID = seg.GL_Product_Code
 AND UPPER(IFNULL(A.Oracle_Product,'Unknown'))<>UPPER(IFNULL(seg.GL_Product_Description,'Unknown'));
------------------------
--GL_Account_DESC
------------------------
UPDATE  stage_two_dw.stage_dedicated_inv_event_detail A
SET  
    Oracle_GL_Account_DESC = IFNULL(GL_Account_Description, 'Unknown') 
FROM stage_two_dw.stage_dedicated_gl_codes seg
WHERE A.GL_Account = seg.GL_Code
AND      UPPER(IFNULL(A.Oracle_GL_Account_DESC, 'Unknown'))<> UPPER(IFNULL(GL_Account_Description, 'Unknown') );
------------------------------------------------------
UPDATE  stage_two_dw.stage_dedicated_inv_event_detail  A
SET  
    Trx_Line_NK = cast(CONCAT(CAST(master_unique_id as STRING),'--',CAST(gl_Account as STRING),'--',CAST(Time_Month_Key as STRING),'--',CAST(IFNULL(Server,'1') as STRING),'--',(case when Trx_Term = '0' then CAST(Trx_Qty AS STRING) else CAST(trx_term as STRING) end)) as string)

WHERE
    Trx_Line_NK<>cast(CONCAT(CAST(master_unique_id as STRING),'--',CAST(gl_Account as STRING),'--',CAST(Time_Month_Key as STRING),'--',CAST(IFNULL(Server,'1') as STRING),'--',(case when Trx_Term = '0' then CAST(Trx_Qty AS STRING) else CAST(trx_term as STRING) end)) as string)
	;


END;
