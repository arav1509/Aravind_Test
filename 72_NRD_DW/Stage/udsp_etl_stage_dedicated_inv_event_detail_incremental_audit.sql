CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_stage_dedicated_inv_event_detail_incremental_audit`()
BEGIN

-------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE  stage_one.raw_ded_sessionlist_audit AS
select distinct 
    EVENT_Session_obj_ido  AS session_obj_id0   
from 
    stage_two_dw.stage_dedicated_inv_event_detail_incremental_audit_inv_detail_audit --#INV_DETAIL_Audit 
where 
    Trx_Bill_Pay_Site_ID =0;
-------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------
-- GET GL DETAILS
create or replace table     stage_two_dw.raw_dedicated_inv_event_detail_stage_audit  as 
select
 a.master_unique_id
,a.Trx_Line_NK
,a.Trx_ID
,a.Trx_Line_ID
,a.Trx_Line_GL_Dist_ID
,a.Trx_Resource_ID
,a.Trx_Number
,a.Trx_Complete_Flag
,a.Trx_Bill_Pay_Site_ID
,a.Invoice_WKST_SNID
,a.Trx_Unit_Of_Measure_Code
,a.Trx_Raw_Start_Date
,a.Trx_Qty
,a.Trx_Term
,a.Trx_Description
,a.Account
,a.Server
,a.Organization
,a.Oracle_Department_ID
,a.Dedicated_Oracle_Department_ID
,a.Oracle_Department
,
case when ( UPPER(Oracle_Product_ID) IN ('TRIC','TC_B','AWS_')) 
	OR UPPER(Oracle_Product_ID) like '%DATA%'
	then a.glsub7_product
	else a.Oracle_Product_ID
end as Oracle_Product_ID
,
case when UPPER(Oracle_Product_ID) like '%DATA%'
	then a.glsub7_product
	else a.Dedicated_Oracle_Product_ID 
end as 	Dedicated_Oracle_Product_ID
,a.Oracle_Product
,a.Oracle_Business_Unit_ID
,a.Dedicated_Business_Unit_ID
,a.Oracle_Business_Unit
,a.Oracle_Team_ID
,a.Dedicated_Oracle_Team_ID
,a.Oracle_Team
,a.Oracle_Company_ID
,a.Dedicated_Oracle_Company_ID
,a.Oracle_Company
,a.Oracle_Location_ID
,a.Dedicated_Oracle_Location_ID
,a.Oracle_Location
,a.GL_Account
,a.Oracle_GL_Account_DESC
,a.DED_usage_type
,a.Dedicated_Product_Group
,a.Dedicated_Product_Type
,a.product_poid
,a.product_type
,a.product_descr
,a.product_name
,a.product_code
,a.prod_permitted
,a.product_type2
,a.Invoice_Date
,a.Time_Month_Key
,a.TOTAL
,a.Extended_Amount
,a.Billing_Days_In_Month
,a.Currency_ID
,a.currency_abbrev
,a.currency_name
,a.Refresh_Date
,a.print_group
,a.Trx_Date
,a.Unit_Selling_Price
,a.event_poid
,a.EVENT_Item_Obj_Id0
,a.event_mod_dtt
,a.ebi_rec_id
,a.event_type
,a.item_type
,a.item_poid
,a.ITEM_NO
,a.ITEM_name
,a.Item_Effective_Date
,a.ITEM_Bill_Obj_Id0
,a.service_obj_type
,a.service_obj_id0
,a.gl_id
,a.session_obj_id0
, CASE WHEN (
				UPPER(Trx_Description) ='SALES TAX ONLY INCORRECT TAX RATE'
				AND Misc_Event_Billing_Type_Reason_ID=97
				AND Misc_Event_Billing_Type IS NULL
			)
		THEN 'Sales Tax Only'
	ELSE  a.MISC_EVENT_BILLING_Type
	END AS MISC_EVENT_BILLING_Type
,a.MISC_EVENT_BILLING_Type_Reason_ID
,a.MISC_EVENT_BILLING_Type_record_id
, CASE WHEN (
				UPPER(Trx_Description) ='SALES TAX ONLY INCORRECT TAX RATE'
				AND Misc_Event_Billing_Type_Reason_ID=97
				AND Misc_Event_Billing_Type IS NULL
			)
		THEN 'Reason Codes-Debit Reasons'
	ELSE  a.string_domain
 END AS 	string_domain
, CASE WHEN (
				UPPER(Trx_Description) ='SALES TAX ONLY INCORRECT TAX RATE'
				AND Misc_Event_Billing_Type_Reason_ID=97
				AND Misc_Event_Billing_Type IS NULL
			)
		THEN 1
	ELSE a.string_version
 END AS string_version
,a.service_login_site_id
,a.ded_login_siteid
,a.BRM_ACCOUNT_NO
,a.account_poid
,a.Payment_Term
,a.Payment_Term_DESC
,a.GL_SEGMENT
,a.BUSINESS_TYPE
,a.BUSINESS_TYPE_DESCR
,a.RECORD_ID
,a.REGION
,a.Bill_NO
,a.ded_bill_end
,a.ded_bill_start
,a.BILL_START_DATE
,a.BILL_END_DATE
,a.BILL_MOD_DATE
,a.ded_prepay_start
,a.ded_prepay_end
,a.profile_bu
, CASE WHEN (
				UPPER(Trx_Description) ='SALES TAX ONLY INCORRECT TAX RATE'
				AND Misc_Event_Billing_Type_Reason_ID=97
				AND Misc_Event_Billing_Type IS NULL
			)
		THEN 35238450545
	ELSE a.glid_obj_id0
 END AS 	glid_obj_id0	
,CASE WHEN (
				UPPER(Trx_Description) ='SALES TAX ONLY INCORRECT TAX RATE'
				AND Misc_Event_Billing_Type_Reason_ID=97
				AND Misc_Event_Billing_Type IS NULL
			)
		THEN 35238452593
 ELSE a.GLSeg_poid
 END AS GLSeg_poid
,CASE WHEN (
				UPPER(Trx_Description) ='SALES TAX ONLY INCORRECT TAX RATE'
				AND Misc_Event_Billing_Type_Reason_ID=97
				AND Misc_Event_Billing_Type IS NULL
			)
		THEN  'ADJ Sales Tax Only'
	ELSE a.glid_descr
END AS 	glid_descr
,a.glid_rec_id
,a.GLAcct_rec_id
,a.GLAcct_attribute
,a.Attribute_Descr
,a.GLAcct_record_type
,a.Report_Type_Name
,a.GLAcct_offset_acct
,a.GLAcct_ar_acct
,a.glsub1_company
,a.glsub2_locationdc
,a.glsub3_acct_subprod
,a.glsub4_team
,a.glsub5_busunit
,a.glsub6_dept
,a.glsub7_product
,a.master_tbl_loaddtt
,upper(a.LINE_OF_BUSINESS) as  LINE_OF_BUSINESS
,a.Event_Create_Date
,a.EVENT_FASTLANE_SERVICE_TYPE
,a.EVENT_FASTLANE_EVENT_TYPE
,a.EVENT_FASTLANE_RECORD_ID
,a.EVENT_FASTLANE_DC_ID
,a.EVENT_FASTLANE_REGION
,a.EVENT_FASTLANE_RESOURCE_ID
,a.EVENT_FASTLANE_RESOURCE_NAME
,a.EVENT_FASTLANE_ATTR1
,a.EVENT_FASTLANE_ATTR2
,a.EVENT_FASTLANE_ATTR3
,a.FASTLANE_IMPACT_CATEGORY
,a.FASTLANE_IMPACT_VALUE
,a.IMPACT_CATEGORY
,a.IMPACT_TYPE
,
case when (EVENT_earned_start_dtt IS NULL)
OR (EVENT_earned_start_dtt= '1970-01-01')
	then '1900-01-01'
	else a.EVENT_earned_start_dtt
end as EVENT_earned_start_dtt
,
case when (EVENT_earned_end_dtt IS NULL)
OR (EVENT_earned_end_dtt= '1970-01-01')
then '1900-01-01'
else a.EVENT_earned_end_dtt
end as EVENT_earned_end_dtt

,a.EVENT_FASTLANE_IS_BACKBILL
,a.FASTLANE_INV_DEAL_CODE
,a.FASTLANE_INV_GRP_CODE
,a.FASTLANE_INV_SUB_GRP_CODE
,a.FASTLANE_INV_Is_Backbill
,a.EBI_OFFERING_OBJ_ID0
, case when (IFNULL(Is_Backbill,0)=0)
			and 
			(
				 ((CAST(EVENT_earned_end_dtt as date) <> '1970-01-01') AND IfNULL(EVENT_earned_end_dtt,'1970-01-01') <= IfNULL(Bill_Start_Date,'1970-01-01'))
			  OR (IFNULL(FASTLANE_INV_Is_Backbill,0)<>0) 
			  OR (IFNULL(EVENT_FASTLANE_IS_BACKBILL,0)<>0)	
			  OR (IFNULL(upper(FASTLANE_INV_SUB_GRP_CODE ),'UNKNOWN') like  '%BACKBILL%'
			)
			)
	then 1
	else a.Is_Backbill
   end as Is_Backbill
,a.Event_Start_Dt
,a.Event_End_Date
,a.ACTIVITY_ATTR4
,a.ACTIVITY_ATTR5
,a.ACTIVITY_ATTR6
,a.EBI_rate_tag
,a.Tax_Type_Id
,a.Tax_Element_Id
,a.RATE
,a.Impact_Type_Description
,a.Bill_Created_Date
from
(
SELECT
    ded.master_unique_id,
    CAST('0' AS STRING)	AS Trx_Line_NK,
    ded.Trx_ID,
    ded.Trx_Line_ID,
    ded.Trx_Line_GL_Dist_ID,
    ded.Trx_Resource_ID,
    IFNULL(ded.Trx_Number,'Unknown') AS Trx_Number,
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
    coalesce(glsub6_dept,Oracle_Department_ID,'0000')	 AS Oracle_Department_ID,/* not in gl segs-all0000; not in brm ---- set to default when not found*/ 
    Oracle_Department_ID AS Dedicated_Oracle_Department_ID,
    ded.Oracle_Department,-- default unknown
    coalesce(GL.glsub7_product,ded.Oracle_Product_ID,'0000')  AS Oracle_Product_ID,
    Oracle_Product_ID	AS Dedicated_Oracle_Product_ID,
    ded.Oracle_Product,-- default unknown
    coalesce(glsub5_busunit,Oracle_Business_Unit_ID,'0000') AS Oracle_Business_Unit_ID, /* not in gl segs-all0000; not in brm*/ 
    Oracle_Business_Unit_ID	AS Dedicated_Business_Unit_ID,
    ded.Oracle_Business_Unit,-- default unknown
    coalesce(ded.Oracle_Team_ID,GL.glsub4_team,'000')	AS Oracle_Team_ID,/* not in gl segs-all000*/
    Oracle_Team_ID	AS Dedicated_Oracle_Team_ID,
    ded.Oracle_Team,-- from ded; default null
    coalesce(ded.Oracle_Company_ID,GL.glsub1_company,'000') AS Oracle_Company_ID,/* not in gl segs-all000*/
    Oracle_Company_ID AS Dedicated_Oracle_Company_ID,
    ded.Oracle_Company,-- from ded; default null
    coalesce( glsub2_locationdc,Oracle_Location_ID ,'000') AS Oracle_Location_ID,    -- from gl segs
    Oracle_Location_ID	 AS Dedicated_Oracle_Location_ID,
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
    TRUNC(CAST(ded.Unit_Selling_Price as numeric),2)	AS Unit_Selling_Price,
    ------------------------------------------
    EVENT_POID_ID0 AS event_poid,
    EVENT_Item_Obj_Id0,
    DED.event_mod_dtt,
    ded.ebi_rec_id,
    ded.event_type,
    ded.item_type,
    ITEM_POID_ID0  AS item_poid,
    ded.ITEM_NO,
    ded.ITEM_name,
    Item_Effective_Date,
    ITEM_Bill_Obj_Id0,
    EVENT_service_obj_type AS service_obj_type,
    EVENT_service_obj_id0 AS service_obj_id0,
    EBI_GL_ID   AS gl_id,
    EVENT_service_obj_id0  AS session_obj_id0,
    MISC_EVENT_BILLING_Type,			---Credit_Type
    MISC_EVENT_BILLING_Type_Reason_ID,  --Credit_Reason_ID
    MISC_EVENT_BILLING_Type_record_id,
    string_domain,
    string_version,     
    ded.service_login_site_id,
    ded.ded_login_siteid,
    ded.BRM_ACCOUNT_NO,
    ded.account_poid,
    PAYMENT_TERM	AS Payment_Term,
    CAST('Unknown' As STRING) AS Payment_Term_DESC,
    ded.GL_SEGMENT,
    ded.BUSINESS_TYPE,
    ded.BUSINESS_TYPE_DESCR,
    DED_record_id AS RECORD_ID,
    DED_region	AS REGION,
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
    impact_category	AS IMPACT_CATEGORY,
    IMPACT_TYPE	AS IMPACT_TYPE,
    EVENT_earned_start_dtt, 
    EVENT_earned_end_dtt,
    IFNULL(EVENT_FASTLANE_IS_BACKBILL,0) AS EVENT_FASTLANE_IS_BACKBILL,
    FASTLANE_INV_DEAL_CODE,
    FASTLANE_INV_GRP_CODE,
    FASTLANE_INV_SUB_GRP_CODE,
    IFNULL(FASTLANE_INV_Is_Backbill,0)	AS FASTLANE_INV_Is_Backbill,
    EBI_OFFERING_OBJ_ID0,
    0 AS Is_Backbill 
	,Event_Start_dtt AS Event_Start_Dt,
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
FROM
   stage_two_dw.stage_dedicated_inv_event_detail_incremental_audit_inv_detail_audit ded -- #INV_DETAIL_Audit ded
left outer join
    stage_one.raw_brm_glid_account_config gl 
	ON ded.EBI_GL_ID= glid_rec_id
inner join 
    stage_two_dw.stage_brm_glid_acct_atttribute atr
on GL.GLAcct_attribute  = atr.Attribute_id
inner join 
    stage_two_dw.stage_brm_glid_acct_report_type typ
ON GL.GLAcct_record_type=typ.Report_Type_id

AND ded.GL_SEGMENT= GL.GLSeg_name 
AND GL.GLAcct_record_type IN (2,8) -- gl for BILLED events only
AND GL.GLAcct_attribute= 1  -- gl for net amounts only

) a
;
------------------------------------------------------------------------------------------------------------

/** BRM GL SEGMENT DETAILS  **/
/**  Oracle GL Values  ***/
create or replace temp table flex_values as 
	SELECT  --#flex_values
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
		)
;
	

UPDATE   stage_two_dw.raw_dedicated_inv_event_detail_stage_audit   as  ded
SET 
    Oracle_Department = seg.DESCRIPTION
FROM flex_values seg
where ded.Oracle_Department_ID = seg.flex_value
 AND seg.flex_value_set_name='RS_DEPARTMENTS';
--print ('dept updated')
-----------------
--PRODUCT
------------------
UPDATE  stage_two_dw.raw_dedicated_inv_event_detail_stage_audit   as  ded
SET  
    Oracle_Product = seg.DESCRIPTION
FROM flex_values seg
where ded.Oracle_Product_ID = seg.flex_value
 AND seg.flex_value_set_name='RS_PRODUCT'
AND ded.Oracle_Product = 'unknown';

--print ('prod updated')
------------------------
--BUSINESS UNIT
------------------------
UPDATE  stage_two_dw.raw_dedicated_inv_event_detail_stage_audit   as  ded
SET  
    Oracle_Business_Unit = seg.DESCRIPTION
FROM
    flex_values seg
where ded.Oracle_Business_Unit_ID = seg.flex_value
 AND seg.flex_value_set_name='RS_BUSINESS_UNITS'
AND ded.Oracle_Business_Unit = 'unknown';
--print ('BUsUnit updated')
------------------------
--TEAM
------------------------
UPDATE  stage_two_dw.raw_dedicated_inv_event_detail_stage_audit   as  ded
SET  
    Oracle_Team = seg.DESCRIPTION
FROM
    flex_values seg
where ded.Oracle_Team_ID = seg.flex_value
AND seg.flex_value_set_name='RS_TEAM'
AND ded.Oracle_Team  = 'unknown';
--print ('team updated')
------------------------
--COMPANY
------------------------
UPDATE  stage_two_dw.raw_dedicated_inv_event_detail_stage_audit   as  ded
SET  
    Oracle_Company = seg.DESCRIPTION
FROM
    flex_values seg
where ded.Oracle_Company_ID = seg.flex_value
AND seg.flex_value_set_name='RS_COMPANY'
AND ded.Oracle_Company IS NULL;
--print ('comp updated')
------------------------
--LOCATION
------------------------
UPDATE  stage_two_dw.raw_dedicated_inv_event_detail_stage_audit   as  ded
SET  
    Oracle_Location = seg.DESCRIPTION
FROM
    flex_values seg
where ded.Oracle_Location_ID = seg.flex_value
AND seg.flex_value_set_name='RS_LOCATION'
AND ded.Oracle_Location IS NULL;
--print ('location updated')
------------------------
--GL Account Desc
------------------------
UPDATE   stage_two_dw.raw_dedicated_inv_event_detail_stage_audit  as  ded
SET  
    Oracle_GL_Account_DESC = seg.DESCRIPTION
FROM
    flex_values seg
where ded.GL_Account = seg.flex_value
AND seg.flex_value_set_name='RS_Account'
AND ded.Oracle_GL_Account_DESC ='Unknown';
--print ('GL Account updated')
------------------------------------------------------
--GL Account for tax
UPDATE stage_two_dw.raw_dedicated_inv_event_detail_stage_audit A
SET
    gl_Account='220800'
WHERE
   GL_ACCOUNT='000000' 
AND event_type Like '%tax%';
------------------------------------------------------
UPDATE stage_two_dw.raw_dedicated_inv_event_detail_stage_audit  A
SET
   Dedicated_Product_Group='Tax',
   Dedicated_Product_Type='Tax'
WHERE
    event_type Like '%tax%'
AND Dedicated_Product_Group IS NULL;
------------------------------------------------------
UPDATE stage_two_dw.raw_dedicated_inv_event_detail_stage_audit  A 
SET
   A.Payment_Term_DESC=B.Payment_Term_Description
FROM
   `rax-datamart-dev`.corporate_dmart.dim_payment_terms   B
WHERE A.Payment_Term=B.Payment_Term_Nk;
------------------------------------------------------
/*
UPDATE   stage_two_dw.raw_dedicated_inv_event_detail_stage_audit  A 
SET
   LINE_OF_BUSINESS='DEDICATED'
WHERE
     lower(LINE_OF_BUSINESS)='dedicated';
	 */
	 
UPDATE stage_two_dw.raw_dedicated_inv_event_detail_stage_audit A
SET 	
    Trx_Line_NK=CONCAT(CAST(master_unique_id as STRING),'--',CAST(gl_Account as STRING),'--',CAST(Time_Month_Key as STRING),'--',CAST(IFNULL(Server,'1') as STRING),'--',(case when cast(Trx_Term as string)= '0' then CAST(Trx_Qty AS STRING) else CAST(trx_term as STRING) end))
    where true
;
    

-------------------------------------------------------------------------------------------------------------------


create or replace table     stage_two_dw.stage_dedicated_inv_event_detail_audit_insert as 
SELECT 
    master_unique_id,
    Trx_Line_NK,
    Trx_ID,
    Trx_Line_ID,
    Trx_Line_GL_Dist_ID,
    Trx_Resource_ID,
    ifnull(Trx_Number,'Unknown')						    AS Trx_Number,
    Trx_Complete_Flag,
    Trx_Bill_Pay_Site_ID,
    Invoice_WKST_SNID,
    Trx_Unit_Of_Measure_Code,
    (case 
	   when 
		  Product_type is not null 
	   THEN
		  case when Product_type ='Hosting Service' THEN 'DE' ELSE 'AC' END   -- no logic from sources yet on how to id dev vs acct_jcm_10.12.16
	   else 
		  'Unknown'  
	 END )										    AS Trx_Is_Device_Account,  
    Trx_Raw_Start_Date,
    Trx_Qty,
    Trx_Term,
    Trx_Description,
    Account,
    Server,
    ifnull(Organization,'Unknown')					AS Organization,
    Oracle_Department_ID,
	CASE 
	   WHEN Oracle_Department like '%)%' THEN 
	   LTRIM(RTRIM(REPLACE(SUBSTR(Oracle_Department, STRPOS(Oracle_Department,')'), LENGTH(Oracle_Department)), ')', ' ')))
    ELSE
	   ifnull(Oracle_Department,'Uknown')	
    END					 							AS Oracle_Department,
    Oracle_Product_ID,
	CASE 
	   WHEN Oracle_Product like '%)%' THEN 
	   LTRIM(RTRIM(REPLACE(SUBSTR(Oracle_Product, STRPOS( Oracle_Product,')'), LENGTH(Oracle_Product)), ')', ' ')))
    ELSE
	   ifnull(Oracle_Product,'Uknown')	
    END					 							AS Oracle_Product,
    Oracle_Business_Unit_ID,
	CASE 
	   WHEN Oracle_Business_Unit like '%)%' THEN 
	   LTRIM(RTRIM(REPLACE(SUBSTR(Oracle_Business_Unit, STRPOS(Oracle_Business_Unit,')'), LENGTH(Oracle_Business_Unit)), ')', ' ')))
    ELSE
	   ifnull(Oracle_Business_Unit,'Uknown')	
    END					 							AS Oracle_Business_Unit,
    Oracle_Team_ID,
 	CASE 
	   WHEN Oracle_Team like '%)%' THEN 
	   LTRIM(RTRIM(REPLACE(SUBSTR(Oracle_Team, STRPOS( Oracle_Team,')'), LENGTH(Oracle_Team)), ')', ' ')))
    ELSE
	   ifnull(Oracle_Team,'Uknown')	
    END					 							AS Oracle_Team,
    Oracle_Company_ID,
    CASE 
	   WHEN Oracle_Company like '%)%' THEN 
	   LTRIM(RTRIM(REPLACE(SUBSTR(Oracle_Company, STRPOS( Oracle_Company,')'), LENGTH(Oracle_Company)), ')', ' ')))
    ELSE
	   ifnull(Oracle_Company,'Uknown')	
    END					 							AS Oracle_Company,
    Oracle_Location_ID,
  	CASE 
	   WHEN Oracle_Location like '%)%' THEN 
	   LTRIM(RTRIM(REPLACE(SUBSTR(Oracle_Location, STRPOS(  Oracle_Location,')'), LENGTH(Oracle_Location)), ')', ' ')))
    ELSE
	   ifnull(Oracle_Location,'Uknown')	
    END					 							AS Oracle_Location,
    ifnull(GL_Account,'000000')						AS GL_Account,
	CASE 
	   WHEN Oracle_GL_Account_DESC like '%)%' THEN 
	   LTRIM(RTRIM(REPLACE(SUBSTR(Oracle_GL_Account_DESC, STRPOS( Oracle_GL_Account_DESC,')'), LENGTH(Oracle_GL_Account_DESC)), ')', ' ')))
    ELSE
	   ifnull(Oracle_GL_Account_DESC,'Uknown')	
    END					 							AS Oracle_GL_Account_DESC,
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
    TOTAL,
    Extended_Amount,
    Billing_Days_In_Month,
    Currency_ID,
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
    Item_Effective_Date,
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
    BUSINESS_TYPE,
    BUSINESS_TYPE_DESCR,
    RECORD_ID,
    REGION,
    Payment_Term,
    Payment_Term_DESC,
    Bill_NO,
    ifnull(ded_bill_end,'1900-01-01')	    AS ded_bill_end,			 -- used for pre-pays
    ifnull(ded_bill_start,'1900-01-01')	    AS ded_bill_start,			  -- used for pre-pays
    ifnull(ded_prepay_start,'1900-01-01')	    AS ded_prepay_start,	  -- new field coming to event dedicated data table_no target date yet_11.14.16jcm
    ifnull(ded_prepay_end,'1900-01-01')	    AS ded_prepay_end, -- new field coming to event dedicated data table_no target date yet_11.14.16jcm
    ifnull(BILL_START_DATE,'1900-01-01')	    AS BILL_START_DATE,		 -- used for pre-pays
    ifnull(BILL_END_DATE,'1900-01-01')	    AS BILL_END_DATE,		  -- used for pre-pays
    ifnull(BILL_MOD_DATE,'1900-01-01')	    AS BILL_MOD_DATE,
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
    CURRENT_DATETIME()						AS etldtt,
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
    IMPACT_TYPE,
    EVENT_earned_start_dtt, 
    EVENT_earned_end_dtt,
    EVENT_FASTLANE_IS_BACKBILL,
    FASTLANE_INV_DEAL_CODE,
    FASTLANE_INV_GRP_CODE,
    FASTLANE_INV_SUB_GRP_CODE,
    FASTLANE_INV_Is_Backbill,
    EBI_OFFERING_OBJ_ID0,
    Is_Backbill,
	Event_Start_Dt,
	Event_End_Date,
	ACTIVITY_ATTR4, -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR5, -- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	ACTIVITY_ATTR6,	-- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	EBI_rate_tag, ---- new field added 7.10.2019 anil2912 as per uday request for NRD Staging
	Tax_Type_Id,		 --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Tax_Element_Id,      --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	RATE,				--new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Impact_Type_Description,  --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	Bill_Created_Date         --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging   
FROM 
    stage_two_dw.raw_dedicated_inv_event_detail_stage_audit   ded  ;   


	DELETE FROM 
		stage_two_dw.stage_dedicated_inv_event_detail
	WHERE
	   EXISTS
	(
	SELECT
		master_unique_id 
		FROM stage_two_dw.raw_dedicated_inv_event_detail_stage_audit   XX
	WHERE
		XX.master_unique_id =stage_dedicated_inv_event_detail.master_unique_id
	);
	-------------------------------------------------------------------------------------------------------------------
	DELETE FROM stage_two_dw.stage_dedicated_inv_event_detail
	WHERE
	   EXISTS
	(
	SELECT
		Trx_Number
	FROM
		 
	   stage_two_dw.raw_dedicated_inv_event_detail_stage_audit   XX
	WHERE
		XX.Trx_Number =stage_dedicated_inv_event_detail.Trx_Number
	);
	-------------------------------------------------------------------------------------------------------------------
	INSERT INTO 
		stage_two_dw.stage_dedicated_inv_event_detail
		(master_unique_id
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
		  ,EVENT_FASTLANE_SERVICE_TYPE	-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_EVENT_TYPE		-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_RECORD_ID		-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_DC_ID			-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_REGION			-- new field added 1.20.17_kvc				
		  ,EVENT_FASTLANE_RESOURCE_ID		-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_RESOURCE_NAME	-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_ATTR1		-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_ATTR2		-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_ATTR3			-- new field added 1.20.17_kvc
		  ,FASTLANE_IMPACT_CATEGORY		-- new field added 1.20.17_kvc	
		  ,FASTLANE_IMPACT_VALUE			-- new field added 1.20.17_kvc 
		  ,IMPACT_CATEGORY
		  ,IMPACT_TYPE
		  ,Event_Start_Dt
		  ,Event_End_Date
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
	,RATE				--new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	,Impact_Type_Description  --new field added 7.23.2019 rahu4260 as per uday request for NRD Staging
	,Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
	,Is_Transaction_Successful
	,Global_Account_Type
	,Item_Tag)
	SELECT master_unique_id
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
		  ,cast(Trx_Term as string) as Trx_Term
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
		  ,cast(TOTAL as NUMERIC ) as TOTAL
		  ,cast(Extended_Amount as NUMERIC ) as Extended_Amount
		  ,Billing_Days_In_Month
		  ,cast(Currency_ID as string)  as Currency_ID
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
		  ,cast(Item_Effective_Date as string) Item_Effective_Date
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
		  ,cast( Business_Type as string) as  Business_Type
		  ,Business_Type_DESCR
		  ,Record_ID
		  ,Region
		  ,cast( Payment_Term  as INT64 ) as Payment_Term
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
		  ,EVENT_FASTLANE_SERVICE_TYPE	-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_EVENT_TYPE		-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_RECORD_ID		-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_DC_ID			-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_REGION			-- new field added 1.20.17_kvc				
		  ,EVENT_FASTLANE_RESOURCE_ID		-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_RESOURCE_NAME	-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_ATTR1		-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_ATTR2		-- new field added 1.20.17_kvc
		  ,EVENT_FASTLANE_ATTR3			-- new field added 1.20.17_kvc
		  ,FASTLANE_IMPACT_CATEGORY		-- new field added 1.20.17_kvc	
		  ,FASTLANE_IMPACT_VALUE			-- new field added 1.20.17_kvc 
		  ,IMPACT_CATEGORY
		  ,cast(IMPACT_TYPE as STRING ) as  IMPACT_TYPE
		  ,Event_Start_Dt
		  ,Event_End_Date
		  ,EVENT_earned_start_dtt
		  ,EVENT_earned_end_dtt
		  ,EVENT_FASTLANE_IS_BACKBILL
		  ,FASTLANE_INV_DEAL_CODE
		  ,FASTLANE_INV_GRP_CODE
		  ,FASTLANE_INV_SUB_GRP_CODE
		  ,FASTLANE_INV_Is_Backbill
		  ,Is_Backbill
		  ,EBI_OFFERING_OBJ_ID0,
			TO_BASE64(MD5( CONCAT( BILL_NO, '|',
			ACCOUNT_POID,'|',
			Trx_Unit_Of_Measure_Code,'|',
			impact_category ,'|',
			IMPACT_TYPE,'|',
			Trx_Description ,'|',
			Service_Obj_Type,'|'     
			) ) ) AS Invoice_Nk 
	 ,'Bill_Number|Billing_Application_Account_Number|Unit_Measure_Of_Code|Impact_Category|Impact_Type_Id|Impact_Type_Description|Service_Type' Invoice_Source_field_NK    
	,TO_BASE64(MD5(
	CONCAT(
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
	) ))  AS Invoice_Attribute_NK
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
	, cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(Bill_Created_Date  as int64) second)  as datetime) As Bill_Created_Date   --new field added 7.31.2019 rahu4260 as per uday request for NRD Staging
	,0 AS Is_Transaction_Successful
	,case when upper(ded.line_of_business)='DEDICATED' then 'Managed_Hosting'
	when upper(ded.line_of_business)='DATAPIPE' then 'Datapipe'
	when upper(ded.line_of_business) like '%CLOUD%' then 'Cloud'
	when upper(ded.line_of_business) like '%EMAIL%' then 'Mailtrust'
	else '' end	AS Global_Account_Type,
	CASE 
	WHEN LOWER(ded.Item_Type) = '/item/adjustment' THEN 'Adjustment'
	WHEN LOWER(ded.Item_Type) = '/item/payment' THEN 'Payment'
	WHEN LOWER(ded.Item_Type) = '/item/payment/reversal' THEN 'Reversal'
	WHEN LOWER(ded.Item_Type) = '/item/purchase' THEN 'Purchase'
	WHEN LOWER(ded.Item_Type) = '/item/cycle_tax' THEN 'Tax'
	ELSE NULL END AS Item_Tag
	 FROM 
		stage_two_dw.stage_dedicated_inv_event_detail_audit_insert ded
	WHERE
		NOT EXISTS (SELECT master_unique_id FROM  stage_two_dw.stage_dedicated_inv_event_detail XX  WHERE XX.master_unique_id=ded.master_unique_id);

END;
