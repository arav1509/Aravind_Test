CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_stage_email_apps_inv_event_detail`()
BEGIN

/******************** CHANGE LOG ***********************************
AUDIT: 

User         Date                               Notes

Naga3062     2019/09/26                Initial SP

Vish5196     2019/10/04                Add fields Invoice_Nk and Invoice_Source_Field_NK 
*****************************************************************************/
------------------------------------------------------------------------------------------------------------------------
DECLARE Begindate datetime;
DECLARE Enddate DATETIME;
DECLARE MAXDATE datetime;
DECLARE BEGINTDATE_UNIX  INT64;
DECLARE ENDTDATE_UNIX  INT64;
DECLARE Current_Month  INT64;
DECLARE TSQL STRING; 
------------------------------------------------------------------------------------------------------------------------
SET MAXDATE =(SELECT MAX(Load_Date) FROM stage_two_dw.stage_email_apps_inv_event_detail) ;
SET Begindate = DATE_ADD( MAXDATE,INTERVAL -2 YEAR) ;--(CAST(DATEADD(day, -2, @MAXDATE) as DATE))
SET Enddate =  bq_functions.udf_lastdayofmonth(CURRENT_DATETIME());
SET BEGINTDATE_UNIX = DATETIME_DIFF(DATETIME(Begindate),cast("1970-01-01" as DATE), SECOND); --DATEDIFF(second,{d '1970-01-01'},@Begindate )
SET ENDTDATE_UNIX = DATETIME_DIFF(DATETIME(Enddate),cast("1970-01-01" as DATE), SECOND); --DATEDIFF(second,{d '1970-01-01'},@Enddate )
-----------------------------------------------------------------------------------------------------------------------


SELECT DISTINCT  --INTO    #Account_Info
    ACCOUNT_NO,
    CURRENCY				AS Currency_Code,
    CANON_COMPANY,
    GL_SEGMENT,
    COMPANY
FROM (
SELECT DISTINCT
    ACCOUNT_NO,
    C.CURRENCY,
    CANON_COMPANY,
    GL_SEGMENT,
    COMPANY
FROM 
	`rax-landing-qa`.brm_ods.account_t  A 
INNER JOIN
	`rax-landing-qa`.brm_ods.account_nameinfo_t B  
ON POID_ID0=OBJ_ID0
AND GL_SEGMENT Like lower('%.emailapps%')
LEFT OUTER JOIN
	`rax-landing-qa`.brm_ods.ifw_currency  C  
ON A.CURRENCY=C.CURRENCY_ID
WHERE
	COMPANY IS NOT NULL);
	
	

-------------------------------------------------------------------------------------------------------------------------
create or replace table stage_one.raw_email_apps_daily_invoicing_item_t as 
SELECT 
    *    
FROM 
    (
SELECT 
    Acct.ACCOUNT_NO,
    Bill.BILL_NO		AS LEDGER_BILL_NO,
    Bill.BILL_NO	     AS BILL_NO,
    Bill.POID_ID0		AS Bill_POID_ID0,
    Bill.END_T		     AS BILL_DATE,
    Bill.MOD_T			AS Bill_MOD_DATE,
	Bill.START_T        AS BILL_START_DATE,
	BILL.CREATED_T      AS BILL_CREATED_DATE,
    IT.NAME			AS ITEM_NAME,
    IT.ITEM_NO,
    IT.POID_ID0		AS ITEM_POID_ID0,  
    IT.ITEM_TOTAL,
    IT.GL_SEGMENT,
    IT.EFFECTIVE_T
FROM
    `rax-landing-qa`.brm_ods.item_t IT 
INNER JOIN
    `rax-landing-qa`.brm_ods.account_t acct 
ON IT.ACCOUNT_OBJ_ID0= acct.Poid_Id0
AND lower(IT.GL_SEGMENT) Like '%.emailapps%'
LEFT OUTER JOIN
	`rax-landing-qa`.brm_ods.bill_t  Bill
ON IT.BILL_OBJ_ID0=Bill.POID_ID0
WHERE
	acct.ACCOUNT_NO not like '%-222512%'
AND lower(IT.POID_TYPE) like '%sa3%' or lower(IT.POID_TYPE) like '%adjustment%'
AND IT.MOD_T >=  BEGINTDATE_UNIX);


--DELETE FROM  TABLE stage_one.raw_email_apps_daily_invoicing_event_t WHERE TRUE;

--********************************************************************************************************************* 
CREATE OR REPLACE TABLE   stage_one.raw_email_apps_daily_invoicing_event_t as 
SELECT
    EVENT_POID_ID0,
    EARNED_START_T,
    EARNED_END_T,
    ITEM_POID_ID0,
    EVENT_DESCR
	--IT.
FROM 
   (     
SELECT
    E.POID_ID0		AS EVENT_POID_ID0,
    e.EARNED_START_T,
    e.EARNED_END_T,
    ITEM_POID_ID0,
    e.DESCR		    AS EVENT_DESCR
FROM
  stage_one.raw_email_apps_daily_invoicing_item_t IT 
INNER JOIN
	`rax-landing-qa`.brm_ods.event_t e    
ON IT.ITEM_POID_ID0=E.Item_Obj_Id0
) ;


--********************************************************************************************************************* 
CREATE OR REPLACE TABLE    stage_one.raw_email_apps_daily_credit_event_t AS 
SELECT
    CREDIT_EVENT_POID_ID0,
    Credit_Type, 
    Credit_Reason_ID, 
    Credit_Version_ID,
    Credit_Reason
FROM 
    (   
SELECT
    EBM.OBJ_ID0		AS CREDIT_EVENT_POID_ID0,
    S.STRING			AS Credit_Type, 
    EBM.REASON_ID	     AS Credit_Reason_ID, 
    REASON_DOMAIN_ID     AS Credit_Version_ID,
    EVENT_DESCR	     as Credit_Reason
FROM
    stage_one.raw_email_apps_daily_invoicing_event_t E  
INNER JOIN
    `rax-landing-qa`.brm_ods.event_billing_misc_t EBM   
ON E.EVENT_POID_ID0 = EBM.OBJ_ID0 
INNER JOIN
    `rax-landing-qa`.brm_ods.strings_t  S   
ON EBM.REASON_ID = S.STRING_ID 
AND EBM.REASON_DOMAIN_ID = S.VERSION
AND lower(S.domain) LIKE 'reason%' 
);

create or replace table stage_one.raw_email_apps_daily_invoicing_impact_t  as 
SELECT 
    RESOURCE_ID,
    GL_ID,
    ITEM_DESCRIPTION,
    ITEM_NAME,	
    PREPAY_TERM,
    PRODUCT_POID_ID0,        
    IMPACTBAL_EVENT_OBJ_ID0,   
    IMPACT_CATEGORY,        
    EBI_IMPACT_TYPE, 
	EBI_QUANTITY,       
    EBI_AMOUNT, 
    EBI_DISCOUNT,       
    EBI_PRODUCT_OBJ_ID0,         
    EBI_CURRENCY_ID,		-- new field added 12.07.15_jcm        
    EBI_GL_ID,
    EBI_ITEM_POID_ID0,
    SERVICE_ID
FROM 
    (   
SELECT
    Ebi.RESOURCE_ID,
    ebi.GL_ID,
    SA3_Item.DESCRIPTION		  AS ITEM_DESCRIPTION,
    SA3_Item.ITEM_NAME,	
    SA3_Item.PREPAY_TERM,
    SA3_Item.SERVICE_ID,
    ebi.PRODUCT_OBJ_ID0       AS PRODUCT_POID_ID0,        
    ebi.OBJ_ID0			  AS IMPACTBAL_EVENT_OBJ_ID0,
    EBI.IMPACT_CATEGORY		  AS IMPACT_CATEGORY,        
    EBI.IMPACT_TYPE			  AS EBI_IMPACT_TYPE,    
	EBI.QUANTITY              AS EBI_QUANTITY,    
    EBI.AMOUNT				  AS EBI_AMOUNT, 
    EBI.DISCOUNT			  AS EBI_DISCOUNT,       
    ebi.PRODUCT_OBJ_ID0       AS EBI_PRODUCT_OBJ_ID0,         
    ebi.resource_ID			  AS EBI_CURRENCY_ID,	  
    ebi.GL_ID				  AS EBI_GL_ID,
    E.ITEM_POID_ID0			  AS EBI_ITEM_POID_ID0
FROM
    stage_one.raw_email_apps_daily_invoicing_event_t E  
INNER JOIN
   `rax-landing-qa`.brm_ods.event_bal_impacts_t ebi 
ON  E.EVENT_POID_ID0=Ebi.Obj_Id0 
LEFT OUTER JOIN
   `rax-landing-qa`.brm_ods.event_activity_rax_sa3_t SA3_Item   
ON E.EVENT_POID_ID0=SA3_Item.obj_id0
WHERE
   ebi.resource_id  < 999 
AND EBI.AMOUNT <> 0   
) ;

create or replace table stage_one.raw_email_apps_inv_event_detail_master as 
SELECT DISTINCT 
    CONCAT(CAST(A.ITEM_POID_ID0 AS STRING),'-',CAST(EVENT_POID_ID0 AS STRING) ) AS master_unique_id,
    A.ITEM_POID_ID0,
    EVENT_POID_ID0,
     A.ACCOUNT_NO,
    COMPANY AS Account_Name,
    CAST('N/A' as STRING) AS CURRENCY,
    CASE WHEN  
	   BILL_NO  like '%N/A%' THEN IFNULL(BILL_NO,'-- N/A --')
    ELSE
	   BILL_NO
    END AS BILL_NO,
    C.ITEM_NAME,
    A.ITEM_NO,
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01"), INTERVAL cast(A.EFFECTIVE_T  as int64) second)  as datetime) AS Invoice_Date, 
    bq_functions.udf_yearmonth_nohyphen(cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01"), INTERVAL cast(A.EFFECTIVE_T  as int64) second)  as datetime)) AS Invoice_Date_Time_Month_Key,
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01"), INTERVAL cast(EARNED_START_T  as int64) second)  as datetime) AS Pre_Pay_Start_Date,
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01"), INTERVAL cast(EARNED_END_T  as int64) second)  as datetime) AS Pre_Pay_End_Date,
    PREPAY_TERM,
    SERVICE_ID,
    RESOURCE_ID,
    IFNULL(EBI_GL_ID,0) AS EBI_GL_ID,
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01"), INTERVAL cast(A.BILL_CREATED_DATE  as int64) second)  as datetime) as BILL_CREATED_DATE  ,
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01"), INTERVAL cast(A.BILL_START_DATE  as int64) second)  as datetime) as  BILL_START_DATE,
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01"), INTERVAL cast(A.Bill_MOD_DATE  as int64) second)  as datetime)  as  Bill_MOD_DATE,
	cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01"), INTERVAL cast(A.BILL_DATE  as int64) second)  as datetime) as  BILL_END_DATE,
    Credit_Type, 
    Credit_Reason_ID, 
    Credit_Version_ID,
    Credit_Reason,
    A.GL_SEGMENT AS GL_SEGMENT,
    IFNULL(EBI_AMOUNT,0) AS Total,
	IFNULL(C.EBI_QUANTITY,0) AS QUANTITY
FROM
    stage_one.raw_email_apps_daily_invoicing_item_t  A   
LEFT OUTER JOIN
    stage_one.raw_email_apps_daily_invoicing_event_t B
ON A.ITEM_POID_ID0=B.ITEM_POID_ID0
LEFT OUTER JOIN
    stage_one.raw_email_apps_daily_credit_event_t cr
ON B.EVENT_POID_ID0=cr.CREDIT_EVENT_POID_ID0
INNER JOIN
    stage_one.raw_email_apps_daily_invoicing_impact_t C
ON B.EVENT_POID_ID0=C.IMPACTBAL_EVENT_OBJ_ID0
INNER JOIN
	(
		SELECT DISTINCT  --INTO    #Account_Info
			ACCOUNT_NO,
			CURRENCY				AS Currency_Code,
			CANON_COMPANY,
			GL_SEGMENT,
			COMPANY
		FROM (
		SELECT DISTINCT
			ACCOUNT_NO,
			C.CURRENCY,
			CANON_COMPANY,
			GL_SEGMENT,
			COMPANY
		FROM 
			`rax-landing-qa`.brm_ods.account_t  A 
		INNER JOIN
			`rax-landing-qa`.brm_ods.account_nameinfo_t B  
		ON POID_ID0=OBJ_ID0
		AND GL_SEGMENT Like lower('%.emailapps%')
		LEFT OUTER JOIN
			`rax-landing-qa`.brm_ods.ifw_currency  C  
		ON A.CURRENCY=C.CURRENCY_ID
		WHERE
			COMPANY IS NOT NULL)
	) acct --#Account_Info acct 
ON A.ACCOUNT_NO=acct.ACCOUNT_NO
WHERE
    IFNULL(EBI_AMOUNT,0)<>0;
	
	
UPDATE    stage_one.raw_email_apps_inv_event_detail_master d
SET 
    CURRENCY = a.Currency
FROM 
   stage_two_dw.stage_currency_codes a
where d.RESOURCE_ID = a.Currency_ID;
-------------------------------------------------------------------------------------------------------------------    
create or replace temp table Invoice_Data as 
SELECT DISTINCT  --INTO    Invoice_Data	
    master_unique_id,
    ITEM_POID_ID0,
    EVENT_POID_ID0,
    ACCOUNT_NO,
    Account_Name,
    CURRENCY,
    ifnull(BILL_NO,ITEM_NO)										 AS BILL_NO,
    ITEM_NAME,
    ITEM_NO,
    Invoice_Date,
    Invoice_Date_Time_Month_Key,
    Pre_Pay_Start_Date,
    Pre_Pay_End_Date,
    CASE	  
	   WHEN SERVICE_ID LIKE '%:%'  THEN CAST(SUBSTR(SERVICE_ID, STRPOS(SERVICE_ID,':') + 1, length(SERVICE_ID)) as int64)
 	
    ELSE 
	  ceiling(cast(PREPAY_TERM as numeric)) 
    END														 AS  PREPAY_TERM,
    CASE
	   WHEN SERVICE_ID LIKE '%:%'    THEN SUBSTR(SERVICE_ID,1, STRPOS(SERVICE_ID,':')-1)		
    ELSE 
	  SERVICE_ID
    END														 AS  SERVICE_ID,
    RESOURCE_ID,
    EBI_GL_ID,
    GL_SEGMENT,
    ifnull(glid_descr,'Unknown')									 AS GL_Description,
	  A.QUANTITY,
    Total,
    Total														 AS Total_Corrected,
    GLAcct_ar_acct												 AS AR_GROSS_GL_ACCT,
    GLAcct_offset_acct											 AS OFF_GROSS_GL_ACCT,
    cast(GL.glsub3_acct_subprod as STRING)						 AS GL_Account,
    cast(GL.glsub6_dept as STRING)							 AS Oracle_Department_ID,
    cast('unknown'as STRING)									 AS Oracle_Department,
    cast(GL.glsub7_product as STRING)							 AS Oracle_Product_ID,
    cast('unknown'as STRING)									 AS Oracle_Product,
    cast(GL.glsub5_busunit as STRING) 							 AS Oracle_Business_Unit_ID, 
    cast('unknown'as STRING)									 AS Oracle_Business_Unit,
    cast(GL.glsub4_team as STRING)							 AS Oracle_Team_ID,
    cast('unknown'as STRING)									 AS Oracle_Team,
    cast(GL.glsub1_company as STRING)							 AS Oracle_Company_ID,
    cast('unknown'as STRING)									 AS Oracle_Company,
    cast(GL.glsub2_locationdc as STRING)						 AS Oracle_Location_ID,    
    cast('unknown'as STRING)									 AS Oracle_Location,
    Credit_Type, 
    Credit_Reason_ID, 
    Credit_Version_ID,
    Credit_Reason,
	A.BILL_CREATED_DATE  ,
	A.BILL_START_DATE,
	A.Bill_MOD_DATE,
	A.BILL_END_DATE
from
	stage_one.raw_email_apps_inv_event_detail_master  a
left outer join
    stage_one.raw_brm_glid_account_config gl 
on a.gl_segment= gl.glseg_name 
inner join 
   stage_two_dw.stage_brm_glid_acct_atttribute atr  
on gl.glacct_attribute= atr.attribute_id
inner join 
   stage_two_dw.stage_brm_glid_acct_report_type typ
   on gl.glacct_record_type=typ.report_type_id	
where ifnull(a.ebi_gl_id,0)= glid_rec_id
and gl.glacct_record_type in(2,8) -- gl for billed events only
and gl.glacct_attribute= 1 ;
---------------------------------------------------------------------------------------------------------------------

/** BRM GL SEGMENT DETAILS  **/
/**  Oracle GL Values  ***/
create or replace temp table flex_values as 
SELECT  --INTO     flex_values
    flex_value,-- used in update for Oracle ID
    DESCRIPTION,-- used in update for Oracle description
    flex_value_set_name
FROM (
SELECT 
    flex_value,
    ffvt.DESCRIPTION,
    flex_value_set_name
FROM
	`rax-landing-qa`.operational_reporting_oracle.raw_fnd_flex_values    ffv  
left outer join
	`rax-landing-qa`.operational_reporting_oracle.raw_fnd_flex_value_sets   ffvs  
on ffv.flex_value_set_id = ffvs.flex_value_set_id
left outer join
	`rax-landing-qa`.operational_reporting_oracle.raw_fnd_flex_values_tl     ffvt   
on ffv.flex_value_id = ffvt.flex_value_id	
WHERE
	lower(ffvs.flex_value_set_name) IN('rs_team','rs_business_units','rs_departments','rs_company', 'rs_product','rs_location', 'rs_account')
);

UPDATE  Invoice_Data
SET 
    Oracle_Department = (CASE
					     WHEN lower(seg.DESCRIPTION) LIKE '%disabled%'  THEN substr(seg.DESCRIPTION, strpos( seg.DESCRIPTION,')') + 1, length(seg.DESCRIPTION))	
					   ELSE 
					     seg.DESCRIPTION
				      END)	
FROM
    Invoice_Data   as	   email
inner join
    flex_values seg
on email.Oracle_Department_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_DEPARTMENTS'
where true;
-----------------
--PRODUCT
------------------
UPDATE 
    Invoice_Data
SET  
    Oracle_Product =(CASE
				 WHEN lower(seg.DESCRIPTION) LIKE '%disabled%'  THEN substr(seg.DESCRIPTION, strpos(seg.DESCRIPTION,')') + 1, length(seg.DESCRIPTION))	
				ELSE 
				    seg.DESCRIPTION
			     END)	
FROM
    Invoice_Data   as  email
inner join
    flex_values seg
on email.Oracle_Product_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_PRODUCT'
where true;
--9891 updated
--print ('prod updated')
------------------------
--BUSINESS UNIT
------------------------
UPDATE 
    Invoice_Data
SET  
    Oracle_Business_Unit = (CASE
						  WHEN lower(seg.DESCRIPTION) LIKE '%disabled%'  THEN substr(seg.DESCRIPTION, strpos(seg.DESCRIPTION,')') + 1, length(seg.DESCRIPTION))	
					   ELSE 
						  seg.DESCRIPTION
					   END)	
FROM
    Invoice_Data   as  email
inner join
    flex_values seg
on email.Oracle_Business_Unit_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_BUSINESS_UNITS'
where true;
------------------------
--TEAM
------------------------
UPDATE 
    Invoice_Data
SET  
    Oracle_Team = (CASE
				    WHEN lower(seg.DESCRIPTION) LIKE '%disabled%'  THEN substr(seg.DESCRIPTION, strpos(seg.DESCRIPTION,')') + 1, length(seg.DESCRIPTION))	
				ELSE 
				    seg.DESCRIPTION
			    END)	
FROM
    Invoice_Data   as  email
inner join
    flex_values seg
on email.Oracle_Team_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_TEAM'
where true;

------------------------
--COMPANY
------------------------
UPDATE 
    Invoice_Data
SET  
    Oracle_Company =  (CASE
					     WHEN lower(seg.DESCRIPTION) LIKE '%disabled%'  THEN substr(seg.DESCRIPTION, strpos(seg.DESCRIPTION,')') + 1, length(seg.DESCRIPTION))	
				    ELSE 
					     seg.DESCRIPTION
				   END)	
FROM
    Invoice_Data   as  email
inner join
    flex_values seg
on email.Oracle_Company_ID = seg.flex_value
where true;
--print ('comp updated')
------------------------
--LOCATION
------------------------
UPDATE 
    Invoice_Data
SET  
    Oracle_Location =  (CASE
					     WHEN lower(seg.DESCRIPTION) LIKE '%disabled%'  THEN substr(seg.DESCRIPTION, strpos(seg.DESCRIPTION,')') + 1, length(seg.DESCRIPTION))	
				    ELSE 
					     seg.DESCRIPTION
				   END)
FROM
    Invoice_Data   as  email
inner join
    flex_values seg
on email.Oracle_Location_ID = seg.flex_value
AND upper(seg.flex_value_set_name)='RS_LOCATION'
AND upper(email.Oracle_Location)= 'UNKNOWN'
where true;


-----------------------------------------------------------------------------------------------------------
create or replace table     stage_one.raw_email_apps_inv_event_detail as 
SELECT 
    master_unique_id,
    ITEM_POID_ID0,
    EVENT_POID_ID0,
    ACCOUNT_NO										    AS BRM_Account,
    RTRIM(RIGHT(ACCOUNT_NO, length(RTRIM(ACCOUNT_NO))-strpos(ACCOUNT_NO,'-')))	
												    AS Account,
    BILL_NO,
    ITEM_NO,
    A.ITEM_NAME,
    Replace(Replace(Replace(Account_Name,Chr(13),''),Chr(10),''), chr(9),'') AS Account_Name,
    GL_SEGMENT,					     -- new field added 7.27.2018kvc
    A.GL_Account,					     -- new field added 7.27.2018kvc
    Oracle_Department_ID,				-- new field added 7.27.2018kvc  
    Oracle_Department,				     -- new field added 7.27.2018kvc
    Oracle_Product_ID,				     -- new field added 7.27.2018kvc
    Oracle_Product,					     -- new field added 7.27.2018kvc
    Oracle_Business_Unit_ID,				-- new field added 7.27.2018kvc   
    Oracle_Business_Unit,			     -- new field added 7.27.2018kvc
    Oracle_Team_ID,					     -- new field added 7.27.2018kvc
    Oracle_Team,					     -- new field added 7.27.2018kvc
    Oracle_Company_ID,				     -- new field added 7.27.2018kvc
    Oracle_Company,					     -- new field added 7.27.2018kvc
    Oracle_Location_ID,					-- new field added 7.27.2018kvc   
    Oracle_Location, 					-- new field added 7.27.2018kvc
    CASE 
	   WHEN
		  upper(BILL_NO) LIKE '%A1-%'
	   THEN
		  'ADJ'
	   ELSE
		  'INV'
    END											    AS Transaction_Type,
     COALESCE(a.GL_Description   ,b.GL_Description )              AS GL_Description,
     b.GL_Product_Group                                        AS GL_Product_Group,
    IFNULL(GL_Term,1)								    AS GL_Term,
    EBI_GL_ID,
    CURRENCY,
    Invoice_Date,
    Invoice_Date_Time_Month_Key							AS Time_Month_Key,
    Pre_Pay_Start_Date,
    Pre_Pay_End_Date,
    IFNULL(PREPAY_TERM,1)							     AS PREPAY_TERM,
    IFNULL(prod.Service_ID,0)							     AS Service_ID,
    bq_functions.ufn_getdaysinmonth(bq_functions.udf_lastdayofpreviousmonth(cast(Invoice_Date as date)))
												    AS Billing_Days_In_Month,
    AR_GROSS_GL_ACCT,
    OFF_GROSS_GL_ACCT,
	Quantity,
    Total											    AS Total, 
    Total	  										    AS Total_Corrected,
    1											    AS Include_in_payable,
    1											    AS Include_in_QC,	
    0											    AS Is_Prepay,
    Credit_Type, 
    Credit_Reason_ID, 
    Credit_Version_ID,
    Credit_Reason,
	A.BILL_CREATED_DATE  ,
	A.BILL_START_DATE,
	A.Bill_MOD_DATE,
	A.BILL_END_DATE
from  
	Invoice_Data  a
left outer join
	stage_two_dw.stage_cloud_email_apps_gl_codes b
on cast(a.ebi_gl_id as string)=b.gl_code
left outer join
	stage_two_dw.stage_email_apps_brm_products prod
ON A.ITEM_NAME=prod.ITEM_NAME;


UPDATE stage_one.raw_email_apps_inv_event_detail A
SET   
    Total_Corrected=TRUNC(CAST(Total_Corrected/GL_Term AS NUMERIC),2)  
WHERE
    Invoice_Date < date('2019-04-06');
------------------------------------------------------------------------------------------------------------------------
UPDATE  stage_one.raw_email_apps_inv_event_detail A
SET
    Include_In_Payable=0,
    Include_in_QC=0,
    Is_Prepay=1
WHERE	   
   (Pre_Pay_Start_Date IS NOT NULL
AND Pre_Pay_Start_Date <> '1970-01-01 00:00:00.000'
AND Pre_Pay_Start_Date<>Pre_Pay_End_Date
AND PREPAY_TERM <>1 
);
--------------------------------------------------------------------------------------------------------------------
DELETE FROM 
	stage_two_dw.stage_email_apps_inv_event_detail  email
WHERE
    EXISTS (SELECT master_unique_id FROM stage_one.raw_email_apps_inv_event_detail XX WHERE email.master_unique_id=XX.master_unique_id)
	;
------------------------------------------------------------------------------------------------------------------
INSERT INTO stage_two_dw.stage_email_apps_inv_event_detail 
SELECT  
    master_unique_id,
    ITEM_POID_ID0,
    EVENT_POID_ID0,
    BRM_Account,
    Account,
    IFNULL(BILL_NO,ITEM_NO)			    AS BILL_NO,
    ITEM_NO,
    ITEM_NAME,
    Account_Name,
    GL_SEGMENT,					     -- new field added 7.27.2018kvc
    GL_Account,					     -- new field added 7.27.2018kvc
    Oracle_Department_ID,				-- new field added 7.27.2018kvc  
    Oracle_Department,				     -- new field added 7.27.2018kvc
    Oracle_Product_ID,				     -- new field added 7.27.2018kvc
    Oracle_Product,					     -- new field added 7.27.2018kvc
    Oracle_Business_Unit_ID,				-- new field added 7.27.2018kvc   
    Oracle_Business_Unit,			     -- new field added 7.27.2018kvc
    Oracle_Team_ID,					     -- new field added 7.27.2018kvc
    Oracle_Team,					     -- new field added 7.27.2018kvc
    Oracle_Company_ID,				     -- new field added 7.27.2018kvc
    Oracle_Company,					     -- new field added 7.27.2018kvc
    Oracle_Location_ID,					-- new field added 7.27.2018kvc   
    Oracle_Location, 					-- new field added 7.27.2018kvc
    Transaction_Type,
    GL_Description,
    IFNULL(GL_Product_Group,'undefined') AS GL_Product_Group,
    GL_Term,
    EBI_GL_ID,
    CURRENCY,
    Invoice_Date,
    Time_Month_Key,
    Pre_Pay_Start_Date,
    Pre_Pay_End_Date,
    PREPAY_TERM,
    cast(Service_ID as int64) as Service_ID,
    Billing_Days_In_Month,
    cast(Total as numeric) as Total, 
    cast(Total_Corrected as numeric) as Total_Corrected ,
    Include_in_payable,
    Include_in_QC,
    Is_Prepay,
    Credit_Type, 
    Credit_Reason_ID, 
    Credit_Version_ID,
    Credit_Reason,
    current_datetime()						AS Load_Date,
	TO_BASE64(MD5( CONCAT( BILL_NO, '|',
		BRM_Account,'|',
		'N/A','|',
		'N/A' ,'|',
		'N/A','|',
		'N/A' ,'|',
		'N/A','|'     
		) ) ) AS Invoice_Nk,  
'Bill_Number|Billing_Application_Account_Number|Unit_Measure_Of_Code|Impact_Category|Impact_Type_Id|Impact_Type_Description|Service_Type' AS Invoice_Source_field_NK  
,'N/A' as Invoice_Attribute_NK
,'N/A' as Invoice_Attribute_Source_Field_NK
,'Mailtrust' as Global_Account_Type 
,'SA3' as Item_Tag 
,IFNULL(A.BILL_CREATED_DATE,cast('1970-01-01' as datetime )) as Bill_Created_Date
,IFNULL(A.Bill_START_DATE,cast('1970-01-01' as datetime )) as Bill_Start_Date
,IFNULL(A.BILL_END_DATE,cast('1970-01-01' as datetime )) as Bill_End_Date
,IFNULL(A.BILL_MOD_DATE,cast('1970-01-01' as datetime )) as Bill_Mod_Date
,1 as Is_Transaction_Successful,
cast(Quantity as NUMERIC )  as Quantity
FROM
    stage_one.raw_email_apps_inv_event_detail A;

END;
