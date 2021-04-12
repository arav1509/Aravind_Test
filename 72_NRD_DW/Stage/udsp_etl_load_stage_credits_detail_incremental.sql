CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_load_stage_credits_detail_incremental`()
BEGIN

---------------------------------------------------------------------------------------
/*
Created On: 10/15/2019
Created By: hari4586

Description:
Script to load new and modified Credits into Stage_Credits_BRM.

Modified By      Date			Description
-----------------  ----------		----------------------------------------------------------

----------------------------------------------------------------------------------------
*/

----------------------------------------------------------------------------------------

	
DELETE FROM 
	stage_two_dw.stage_credits_brm
WHERE
   EXISTS
(
SELECT
	Item_POID_ID0
FROM
	(
	SELECT DISTINCT --INTO #Modified_Credits
		Item_POID_ID0,
		Event_POID_ID0
	FROM  stage_one.raw_credits_brm_daily_stage A
	) XX--#Modified_Credits XX
WHERE
	XX.Event_POID_ID0=Stage_Credits_BRM.Event_POID_ID0
);

-----------------------------------------------------------------------------------------------------------

INSERT INTO stage_two_dw.stage_credits_brm
(BRM_Account_No
	,Account
	,AccountName
	,EBI_GL_ID
	,Event_POID_ID0
	,Item_POID_ID0
	,Item_POID_Type
	,BILL_OBJ_ID0
	,Credit_EFFECTIVE_DATE
	,Credit_MOD_DATE
	,Credit_Type
	,Credit_Reason_ID
	,Credit_Reason
	,TOTAL
	,tblload_dtt
	,GL_Segment
	,ITEM_NO
	,CURRENCY_ID
	,Credit_Version_ID
--------------------------------------New field introduced -Starts here------------------------------------
	,Transaction_Type
	,Transaction_Type_Description
	,Product_Group
	,Is_Stand_Alone_Fee
	,Normalize
	,Time_Month_Key
	,Create_Date_Time_Month_Key
	,Days_In_Current_Bill_Period
	,Billing_Days_In_Month
	,As_of_Date
	,Currency_Abbrev
	,Total_USD
	,Total_GBP
	,Event_POID_Type
	,Service_Type
	,Quantity
	,Global_Account_Type
	,Item_Tag
	,Invoice_NK
	,glsub3_acct_subprod
	,glsub7_product
	,GLAcct_record_type
	,GLAcct_attribute
--------------------------------------New field introduced - Ends here------------------------------------
	)
SELECT 
	BRM_Account_No, 
	Account, 
	AccountName, 
	EBI_GL_ID, 
	Event_POID_ID0, 
	Item_POID_ID0, 
	Item_POID_Type, 
	BILL_OBJ_ID0, 
	Credit_EFFECTIVE_DATE, 
	Credit_MOD_DATE, 
	Credit_Type, 
	Credit_Reason_ID,
	Credit_Reason, 
	IFNULL(CAST(Total AS NUMERIC),0) AS TOTAL, 
	CURRENT_date()		AS tblload_dtt, 
	GL_Segment, 
	ITEM_NO, 
	A.CURRENCY_ID,
	Credit_Version_ID,
	CAST('CM' as STRING) AS Transaction_Type,
	'CLOUD CREDITS US'		AS Transaction_Type_Description,
	'Credit Memo'			AS Product_Group,
	0						AS Is_Stand_Alone_Fee,
	0						AS Normalize,
	bq_functions.udf_yearmonth_nohyphen(CAST(A.Credit_EFFECTIVE_DATE AS DATE)) AS Time_Month_Key,
	bq_functions.udf_yearmonth_nohyphen(CAST(A.Credit_EFFECTIVE_DATE AS DATE)) AS Create_Date_Time_Month_Key,
	CAST(bq_functions.ufn_getdaysinmonth(bq_functions.udf_lastdayofpreviousmonth(CAST(A.Credit_EFFECTIVE_DATE AS DATE )))as Int64)	AS Days_In_Current_Bill_Period,
	CAST(bq_functions.ufn_getdaysinmonth(bq_functions.udf_lastdayofpreviousmonth(CAST(A.Credit_EFFECTIVE_DATE AS DATE )))as Int64)	AS Billing_Days_In_Month,
	CURRENT_DATE() As_of_Date,
	d.currency AS  CURRENCY_ABBREV,
	IFNULL((A.Total*USDRate.Exchange_Rate_Exchange_Rate_Value),0) AS TOTAL_USD,
	IFNULL((A.Total*GBPRate.Exchange_Rate_Exchange_Rate_Value),0) AS Total_GBP,
	Event_POID_Type,
	Service_Type,
	Quantity,
	'Cloud' AS Global_Account_Type,
	'Adjustment' AS Item_Tag,
	TO_BASE64(MD5( CONCAT( ITEM_NO, '|',
     BRM_Account_No,'|',
     'N/A','|',
     'N/A' ,'|',
     'N/A','|',
     'N/A' ,'|',
     Service_Type,'|'     
     )  ) ) AS Invoice_NK,	
	cast(GL.glsub3_acct_subprod as STRING)AS glsub3_acct_subprod,
	cast(GL.glsub7_product as STRING)       AS glsub7_product,
	cast(GL.GLAcct_record_type  as STRING) AS GLAcct_record_type,
	cast(GL.GLAcct_attribute  as STRING) AS GLAcct_attribute
FROM 
stage_one.raw_credits_brm_daily_stage A 
LEFT JOIN stage_two_dw.stage_currency_codes D 
ON A.Currency_ID=D.Currency_ID
LEFT JOIN stage_one.raw_report_exchange_rate usdrate 
ON d.Currency=USDRate.Exchange_Rate_From_Currency_Code 
	and USDRate.Exchange_Rate_Time_Month_Key=bq_functions.udf_yearmonth_nohyphen(A.Credit_EFFECTIVE_DATE)
	and UPPER(USDRate.Exchange_Rate_To_Currency_Code)='USD'
	and UPPER(USDRate.Source_system_Name)='ORACLE'
LEFT JOIN stage_one.raw_report_exchange_rate gbprate 
ON d.Currency=GBPRate.Exchange_Rate_From_Currency_Code 
	and GBPRate.Exchange_Rate_Time_Month_Key=bq_functions.udf_yearmonth_nohyphen(A.Credit_EFFECTIVE_DATE)
	and UPPER(GBPRate.Exchange_Rate_To_Currency_Code)='GBP'
	and UPPER(GBPRate.Source_system_Name)='ORACLE'
LEFT JOIN
stage_one.raw_brm_glid_account_config GL   -- One time load table so can be used along with Stage one tables
on A.EBI_GL_ID= glid_rec_id
AND A.GL_SEGMENT= GL.GLSeg_name
AND GL.GLAcct_record_type In (2,8) -- gl for BILLED events only
AND GL.GLAcct_attribute= 1
INNER JOIN 
    stage_two_dw.stage_brm_glid_acct_atttribute atr 
on GL.GLAcct_attribute= atr.Attribute_id
INNER JOIN 
    stage_two_dw.stage_brm_glid_acct_report_type typ 
ON GL.GLAcct_record_type=typ.Report_Type_id;
	

END;
