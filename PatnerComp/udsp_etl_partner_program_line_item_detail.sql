CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_program_line_item_detail(V_Date date)
-------------------------------------------------------------------------------------------------------------------
begin
-----------------------------------------------------------------------------------------------------------------------------------------------------------
DECLARE CurrentMonthYear datetime;
DECLARE CurrentTime_Month int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;
-----------------------------------------------------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=V_Date;
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);
-------------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_program_line_item_detail WHERE Invoice_Time_Month_Key = CurrentTime_Month;
---------------------------------------------------------------------------------------------------------------
create or replace temp table Exchange as
SELECT --INTO #Exchange
	Exchange_Rate_From_Currency_Code,
	Exchange_Rate_To_Currency_Code,
	Exchange_Rate_Time_Month_Key,
	Exchange_Rate_Exchange_Rate_Value
FROM
	`rax-abo-72-dev`.net_revenue.report_exchange_rate x
WHERE
	  lower(Source_system_Name) = 'oracle'
  and lower(Exchange_Rate_To_Currency_Code) = 'gbp'
  and Exchange_Rate_Time_Month_Key = CurrentTime_Month;
  
---------------------------------------------------------------------------------------------------------------
create or replace temp table Cloud as
SELECT --INTO	#Cloud
	Opportunity_Number,
	Cloud_Account_Key				as Account_Key,
	Account_number,
	Device_Number,
	Term,
	Final_Opportunity_Type,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	Close_Date,
	Account_Desired_Billing_Date,
	Account_Last_Billing_Date,
	Account_Status,
	Account_Create_Date,
	Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	Oracle_Vendor_ID,
	Tier_Level,
	Points,
	Program,
	INTL_Flag,
	Transaction_Type,
	Product_Group,
	Product_Type,
	ifnull(Fast_lane_Charge_Type,Charge_Type) as Charge_Type,
	B.Time_Month_Key												AS Invoice_Time_Month_Key,
	Currency_Abbrev,
	SUM(TOTAL)													AS TOTAL,
	SUM(TOTAL)													AS TOTAL_USD_GBP,
	'US CLOUD'														AS Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	cast('N/A' as string)										AS Fastlane_Service_Type,
	cast('N/A' as string)										AS Fastlane_Event_Type
FROM 
	`rax-abo-72-dev`.sales.partner_program_accounts_all A 
JOIN
	`rax-abo-72-dev`.slicehost.cloud_invoice_line_item_detail B  
ON A.Account_Number = cast(B.Account as string)
AND A.Time_Month_Key = B.Time_Month_Key
WHERE 
	lower(Account_Source) ='cloud'
AND A.Time_Month_Key =CurrentTime_Month
AND Is_Net_Revenue = 1
GROUP BY
Opportunity_Number,
Cloud_Account_Key,
Account_Number,
Device_Number,
Term,
Final_Opportunity_Type,
Is_Linked_Account,
Is_Consolidated_Billing,
Is_Internal_Account,
Close_Date,
Account_Desired_Billing_Date,
Account_Last_Billing_Date,
Account_Status,
Account_Create_Date,
Account_End_Date,
Account_Tenure,
Partner_Account,
Partner_Role,
Pay_Commissions,
Partner_Account_Name,
Partner_Account_Type,
Partner_Account_Sub_Type,
Partner_Contract_Signed_Date,
Partner_Account_RSA_ID,
Partner_Account_RV_EXT_ID,
Partner_Account_RSA_or_RV,
Partner_Account_Owner,
Partner_Account_Owner_Role,
Partner_Contract_Type,
Commissions_Role,
Oracle_Vendor_ID,
Tier_Level,
Points,
Program,
INTL_Flag,
Transaction_Type,
Product_Group,
Product_Type,
B.Fast_lane_Charge_Type,
B.Charge_Type,
B.Time_Month_Key,
Currency_ID,
Currency_Abbrev,
Partner_Type,
	US_Partner_Type
------------------------------------
UNION ALL
------------------------------------
SELECT
	Opportunity_Number,
	Cloud_Account_Key				as Account_Key,
	Account_number,
	Device_Number,
	Term,
	Final_Opportunity_Type,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	Close_Date,
	Account_Desired_Billing_Date,
	Account_Last_Billing_Date,
	Account_Status,
	Account_Create_Date,
	Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	Oracle_Vendor_ID,
	Tier_Level,
	Points,
	Program,
	INTL_Flag,
	Transaction_Type,
	Product_Group,
	Product_Type,
	ifnull(Fast_lane_Charge_Type,Charge_Type) as Charge_Type,
	B.Time_Month_Key										AS Invoice_Time_Month_Key,
	Currency_Abbrev,
	SUM(TOTAL)											AS TOTAL,
	SUM(TOTAL_USD)											AS TOTAL_USD_GBP,
	'UK CLOUD'												AS Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	cast('N/A' as string)										AS Fastlane_Service_Type,
	cast('N/A' as string)										AS Fastlane_Event_Type
FROM 
	`rax-abo-72-dev`.sales.partner_program_accounts_all A 
JOIN
	`rax-abo-72-dev`.cloud_uk.cloud_invoice_line_item_detail B  
ON A.Account_Number = cast(B.Account as string)
AND A.Time_Month_Key = B.Time_Month_Key
WHERE 
	lower(Account_Source) = 'Cloud'
AND A.Time_Month_Key =CurrentTime_Month
AND Is_Net_Revenue = 1
GROUP BY
Opportunity_Number,
Cloud_Account_Key,
Account_Number,
Device_Number,
Term,
Final_Opportunity_Type,
Is_Linked_Account,
Is_Consolidated_Billing,
Is_Internal_Account,
Close_Date,
Account_Desired_Billing_Date,
Account_Last_Billing_Date,
Account_Status,
Account_Create_Date,
Account_End_Date,
Account_Tenure,
Partner_Account,
Partner_Role,
Pay_Commissions,
Partner_Account_Name,
Partner_Account_Type,
Partner_Account_Sub_Type,
Partner_Contract_Signed_Date,
Partner_Account_RSA_ID,
Partner_Account_RV_EXT_ID,
Partner_Account_RSA_or_RV,
Partner_Account_Owner,
Partner_Account_Owner_Role,
Partner_Contract_Type,
Commissions_Role,
Oracle_Vendor_ID,
Tier_Level,
Points,
Program,
INTL_Flag,
Transaction_Type,
Product_Group,
Product_Type,
Charge_Type,
Fast_lane_Charge_Type,
B.Time_Month_Key,
Currency_Abbrev,
Partner_Type,
	US_Partner_Type
	
	;
-------------------

UPDATE  Cloud c
SET
	c.TOTAL_USD_GBP = A.TOTAL*B.Exchange_Rate_Exchange_Rate_Value
FROM  Cloud A
JOIN Exchange B
ON A.CURRENCY_ABBREV = B.Exchange_Rate_From_Currency_Code
AND a.Invoice_Time_Month_Key = B.Exchange_Rate_Time_Month_Key
WHERE 	A.INTL_Flag = 1;
--------------------------------------------------------------------------------------------------------------
create or replace temp table Dedicated_xx as
SELECT
	Opportunity_Number,
	CONCAT(CAST(Account_Number as string),'','Dedicated')	 AS Account_Key,
	Account_number,
	Device_Number,
	Term,
	Final_Opportunity_Type,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	Close_Date,
	Account_Desired_Billing_Date,
	Account_Last_Billing_Date,
	Account_Status,
	Account_Create_Date,
	Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	Oracle_Vendor_ID,
	Tier_Level,
	Points,
	Program,
	INTL_Flag,
	Transaction_Type,
	B.GL_Account,
	CASE 
		WHEN 
			Oracle_Product_ID = '0000'
		THEN
			DGP00.GL_Product_Code
		ELSE
			DGP.GL_Product_Code
	END																AS GL_Product_Code,
	Oracle_Product												 AS GL_Product_Description,
	
	CASE 
		WHEN 
			Oracle_Product_ID = '0000'
		THEN
			DGP00.GL_Product_Hierarchy
		ELSE
			DGP.GL_Product_Hierarchy
	END													AS GL_Product_Hierarchy,
	CASE 
		WHEN 
			Oracle_Product_ID = '0000'
		THEN
			DGP00.GL_Product_BU
		ELSE
			DGP.GL_Product_BU
	END														AS GL_Product_Focus_Area,
	CASE 
		WHEN 
			Oracle_Product_ID = '0000'
		THEN
			DGP00.GL_Product_Billing_Source
		ELSE
			DGP.GL_Product_Billing_Source
	END														AS GL_Product_Billing_Source,
	B.Product_Group,
	B.Product_Type,
	B.GL_Account_Charge_Type											AS Charge_Type,
	B.Time_Month_Key												AS Invoice_Time_Month_Key,
	Currency,
	SUM(TOTAL)													AS TOTAL,
	SUM(TOTAL_USD)													AS TOTAL_USD_GBP,
	'Dedicated'														AS Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	Fastlane_Service_Type,
	Fastlane_Event_Type
FROM 
	`rax-abo-72-dev`.sales.partner_program_accounts_all A 
LEFT JOIN
	`rax-abo-72-dev`.net_revenue.dedicated_account_invoice_detail B  
ON A.Account_Number = B.Account
AND cast(A.Device_Number as string) = cast(ifnull(B.Server,'0') as string)
AND A.Time_Month_Key = B.Time_Month_Key
LEFT JOIN
	(Select Trx_Line_NK, Time_Month_Key, Fastlane_Service_Type,	Fastlane_Event_Type from `rax-abo-72-dev`.net_revenue.dedicated_account_invoice_detail_brm ) C
ON B.Trx_Line_NK = C.Trx_Line_NK
AND B.Time_Month_Key = C.Time_Month_Key
LEFT OUTER JOIN
    `rax-abo-72-dev`.net_revenue.dim_gl_product_mapping  DGP 
ON B.GL_Product_PK=DGP.GL_Product_PK
LEFT OUTER JOIN
   `rax-abo-72-dev`.net_revenue.dim_dedicated_0000_gl_products  DGP00 
ON B.Oracle_Product_ID=DGP00.GL_Product_Code
AND B.Product_Group=DGP00.Product_Group
AND B.Product_Type=DGP00.Product_Type
WHERE 
	lower(Account_Source) ='dedicated'
AND A.Time_Month_Key =CurrentTime_Month
AND Is_Net_Revenue = 1
--AND B.Product_Type NOT LIKE '%Prior%Period%' 
GROUP BY
	Opportunity_Number,
CONCAT(CAST(Account_Number as string),'','Dedicated'),
Account_Number,
Device_Number,
Term,
Final_Opportunity_Type,
Is_Linked_Account,
Is_Consolidated_Billing,
Is_Internal_Account,
Close_Date,
Account_Desired_Billing_Date,
Account_Last_Billing_Date,
Account_Status,
Account_Create_Date,
Account_End_Date,
Account_Tenure,
Partner_Account,
Partner_Role,
Pay_Commissions,
Partner_Account_Name,
Partner_Account_Type,
Partner_Account_Sub_Type,
Partner_Contract_Signed_Date,
Partner_Account_RSA_ID,
Partner_Account_RV_EXT_ID,
Partner_Account_RSA_or_RV,
Partner_Account_Owner,
Partner_Account_Owner_Role,
Partner_Contract_Type,
Commissions_Role,
Oracle_Vendor_ID,
Tier_Level,
Points,
Program,
INTL_Flag,
Transaction_Type,
B.GL_Account,
CASE 
		WHEN 
			Oracle_Product_ID = '0000'
		THEN
			DGP00.GL_Product_Code
		ELSE
			DGP.GL_Product_Code END,
Oracle_Product,	
CASE 
		WHEN 
			Oracle_Product_ID = '0000'
		THEN
			DGP00.GL_Product_Hierarchy
		ELSE
			DGP.GL_Product_Hierarchy END,
CASE 
		WHEN 
			Oracle_Product_ID = '0000'
		THEN
			DGP00.GL_Product_BU
		ELSE
			DGP.GL_Product_BU
	END,
	CASE 
		WHEN 
			Oracle_Product_ID = '0000'
		THEN
			DGP00.GL_Product_Billing_Source
		ELSE
			DGP.GL_Product_Billing_Source
	END,			
B.Product_Group,
B.Product_Type,
B.GL_Account_Charge_Type,
B.Time_Month_Key,
Currency,
Partner_Type,
	US_Partner_Type,
	Fastlane_Service_Type,
	Fastlane_Event_Type
	;
--------------------------------------------------------------------------------------------------------------
create or replace temp table Dedicated as
	SELECT
		Opportunity_Number,
		Account_Key,
		Account_number,
		Device_Number,
		Term,
		Final_Opportunity_Type,
		Is_Linked_Account,
		Is_Consolidated_Billing,
		Is_Internal_Account,
		Close_Date,
		Account_Desired_Billing_Date,
		Account_Last_Billing_Date,
		Account_Status,
		Account_Create_Date,
		Account_End_Date,
		Account_Tenure,
		Partner_Account,
		Partner_Role,
		Pay_Commissions,
		Partner_Account_Name,
		Partner_Account_Type,
		Partner_Account_Sub_Type,
		Partner_Contract_Signed_Date,
		Partner_Account_RSA_ID,
		Partner_Account_RV_EXT_ID,
		Partner_Account_RSA_or_RV,
		Partner_Account_Owner,
		Partner_Account_Owner_Role,
		Partner_Contract_Type,
		Commissions_Role,
		Oracle_Vendor_ID,
		Tier_Level,
		Points,
		Program,
		INTL_Flag,
		Transaction_Type,
		Product_Group,
		Product_Type,
		Charge_Type,
		Invoice_Time_Month_Key,
		Currency,
		TOTAL,
		TOTAL_USD_GBP,
		Invoice_Source,
		Partner_Type,
		US_Partner_Type,
		Fastlane_Service_Type,
		Fastlane_Event_Type
	FROM Dedicated_xx A
	INNER JOIN
		`rax-abo-72-dev`.net_revenue.dim_dedicated_gl_codes  DGC 
	ON A.GL_Account=DGC.GL_Code
	WHERE
		 ( lower(GL_Product_Hierarchy) in('dedicated','datapipe')
	OR lower(GL_Product_Description) in ('tricore','akamai'))
	--AND GL_Product_Billing_Source='EBS'
	AND GL_Is_Dedicated_Revenue=1	;
---------------------------------------------------------------------------------------------------
UPDATE  Dedicated d
SET
	d.TOTAL_USD_GBP = A.TOTAL*B.Exchange_Rate_Exchange_Rate_Value
FROM	
	Dedicated A
JOIN
	Exchange B
ON A.CURRENCY = B.Exchange_Rate_From_Currency_Code
AND a.Invoice_Time_Month_Key = B.Exchange_Rate_Time_Month_Key
WHERE 	A.INTL_Flag = 1;
---------------------------------------------------------------------------------------------------
create or replace temp table Email as 
SELECT
	Opportunity_Number,
	CONCAT(CAST(Account_Number as string),'','Cloud_Email_Apps_Rack_Indirect' )		 AS Account_Key,
	Account_number,
	Device_Number,
	Term,
	Final_Opportunity_Type,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	Close_Date,
	Account_Desired_Billing_Date,
	Account_Last_Billing_Date,
	Account_Status,
	Account_Create_Date,
	Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	Oracle_Vendor_ID,
	Tier_Level,
	Points,
	Program,
	INTL_Flag,
	Transaction_Type,
	B.Product_Group,
	B.Product_Type,
	B.GL_Account_Charge_Type											AS Charge_Type,
	B.Time_Month_Key												AS Invoice_Time_Month_Key,
	Currency,
	SUM(TOTAL)													AS TOTAL,
	SUM(TOTAL_USD)													AS TOTAL_USD_GBP,
	'Email Indirect'												AS Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	Fastlane_Service_Type,
	Fastlane_Event_Type
FROM 
	`rax-abo-72-dev`.sales.partner_program_accounts_all  A 
LEFT JOIN
	`rax-abo-72-dev`.net_revenue.dedicated_account_invoice_detail B  
ON A.Account_Number = B.Account
AND cast(A.Device_Number as string) = cast(ifnull(B.Server,'0') as string)
AND A.Time_Month_Key = B.Time_Month_Key
LEFT JOIN
	(Select Trx_Line_NK, Time_Month_Key, Fastlane_Service_Type,	Fastlane_Event_Type from `rax-abo-72-dev`.net_revenue.dedicated_account_invoice_detail_brm ) C
ON B.Trx_Line_NK = C.Trx_Line_NK
AND B.Time_Month_Key = C.Time_Month_Key
LEFT OUTER JOIN
   `rax-abo-72-dev`.net_revenue.dim_gl_product_mapping  DGP 
ON B.GL_Product_PK=DGP.GL_Product_PK
WHERE 
	lower(Account_Source) = 'dedicated'
AND A.Time_Month_Key =CurrentTime_Month
AND Is_Net_Revenue = 1
--AND	Product_Type NOT LIKE '%Prior%Period%' 
AND DGP.GL_Product_Hierarchy='Cloud Apps'
--AND DGP.GL_Product_Billing_Source = 'EBS'
AND B.GL_Account<>'120900'
GROUP BY
	Opportunity_Number,
CONCAT(CAST(Account_Number as string),'','Cloud_Email_Apps_Rack_Indirect'),
Account_Number,
Device_Number,
Term,
Final_Opportunity_Type,
Is_Linked_Account,
Is_Consolidated_Billing,
Is_Internal_Account,
Close_Date,
Account_Desired_Billing_Date,
Account_Last_Billing_Date,
Account_Status,
Account_Create_Date,
Account_End_Date,
Account_Tenure,
Partner_Account,
Partner_Role,
Pay_Commissions,
Partner_Account_Name,
Partner_Account_Type,
Partner_Account_Sub_Type,
Partner_Contract_Signed_Date,
Partner_Account_RSA_ID,
Partner_Account_RV_EXT_ID,
Partner_Account_RSA_or_RV,
Partner_Account_Owner,
Partner_Account_Owner_Role,
Partner_Contract_Type,
Commissions_Role,
Oracle_Vendor_ID,
Tier_Level,
Points,
Program,
INTL_Flag,
Transaction_Type,
B.Product_Group,
B.Product_Type,
B.GL_Account_Charge_Type,
B.Time_Month_Key,
Currency,
Partner_Type,
	US_Partner_Type,
	Fastlane_Service_Type,
	Fastlane_Event_Type
------------------------------------
UNION ALL
------------------------------------
SELECT
	Opportunity_Number,
	CONCAT(CAST(Account AS STRING) ,'','Cloud_Email_Apps')	AS Account_Key,
	Account_number,
	Device_Number,
	Term,
	Final_Opportunity_Type,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	Close_Date,
	Account_Desired_Billing_Date,
	Account_Last_Billing_Date,
	Account_Status,
	Account_Create_Date,
	Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	Oracle_Vendor_ID,
	Tier_Level,
	Points,
	Program,
	INTL_Flag,
	Transaction_Type,
    CAST(GL_Product_Group as string) 				AS Product_Group,
	CAST(GL_Description as string) 				AS Product_Type,
	'Recurring'									AS Charge_Type,
	B.Time_Month_Key									AS Invoice_Time_Month_Key,
	Currency,
	SUM(TOTAL)										AS TOTAL,
	SUM(TOTAL)											AS TOTAL_USD_GBP,
	'Email SA3'											AS Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	cast('N/A' as string)										AS Fastlane_Service_Type,
	cast('N/A' as string)										AS Fastlane_Event_Type
FROM 
	`rax-abo-72-dev`.sales.partner_program_accounts_all A 
LEFT JOIN
	`rax-abo-72-dev`.net_revenue.email_apps_account_invoice_detail B
ON A.Account_Number= B.Account
AND A.Time_Month_key = B.Time_Month_Key
WHERE
	lower(Account_SOURCE) = 'email'
AND lower(Program) = 'cloud partner'
AND	Include_in_payable=1
AND upper(CURRENCY) = 'USD'
AND A.Time_Month_Key =CurrentTime_Month
GROUP BY
	Opportunity_Number,
CONCAT(CAST(Account  as string) ,'','Cloud_Email_Apps'),
Account_Number,
Device_Number,
Term,
Final_Opportunity_Type,
Is_Linked_Account,
Is_Consolidated_Billing,
Is_Internal_Account,
Close_Date,
Account_Desired_Billing_Date,
Account_Last_Billing_Date,
Account_Status,
Account_Create_Date,
Account_End_Date,
Account_Tenure,
Partner_Account,
Partner_Role,
Pay_Commissions,
Partner_Account_Name,
Partner_Account_Type,
Partner_Account_Sub_Type,
Partner_Contract_Signed_Date,
Partner_Account_RSA_ID,
Partner_Account_RV_EXT_ID,
Partner_Account_RSA_or_RV,
Partner_Account_Owner,
Partner_Account_Owner_Role,
Partner_Contract_Type,
Commissions_Role,
Oracle_Vendor_ID,
Tier_Level,
Points,
Program,
INTL_Flag,
Transaction_Type,
CAST(GL_Product_Group as string),
CAST(GL_Description as string),
B.Time_Month_Key,
Currency,
Partner_Type,
	US_Partner_Type;
--------------------------------------------------------------------------------------------------------------
UPDATE  Email e
SET
	e.TOTAL_USD_GBP = A.TOTAL*B.Exchange_Rate_Exchange_Rate_Value
FROM		Email A
JOIN	Exchange B
ON A.CURRENCY = B.Exchange_Rate_From_Currency_Code
AND a.Invoice_Time_Month_Key = B.Exchange_Rate_Time_Month_Key
WHERE 	A.INTL_Flag = 1;
----------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_program_line_item_detail
SELECT
	Opportunity_Number,
	Account_Key,
	Account_number,
	Device_Number,
	Term,
	Final_Opportunity_Type,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	Close_Date,
	Account_Desired_Billing_Date,
	Account_Last_Billing_Date,
	Account_Status,
	Account_Create_Date,
	Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	Oracle_Vendor_ID,
	Tier_Level,
	Points,
	Program,
	Transaction_Type,
	Product_Group,
	Product_Type,
	Charge_Type	    AS Product_Type_Charge_Type,
	Invoice_Time_Month_Key,
	TOTAL,
	TOTAL_USD_GBP,
	Invoice_Source,
	INTL_Flag,
	Partner_Type,
	US_Partner_Type,
	Fastlane_Service_Type,
	Fastlane_Event_Type

FROM Cloud
------------------------------------
UNION ALL
------------------------------------
SELECT
	Opportunity_Number,
	Account_Key,
	Account_number,
	Device_Number,
	Term,
	Final_Opportunity_Type,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	Close_Date,
	Account_Desired_Billing_Date,
	Account_Last_Billing_Date,
	Account_Status,
	Account_Create_Date,
	Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	Oracle_Vendor_ID,
	Tier_Level,
	Points,
	Program,
	Transaction_Type,
	Product_Group,
	Product_Type,
	Charge_Type			  AS Product_Type_Charge_Type,
	Invoice_Time_Month_Key,
	TOTAL,
	TOTAL_USD_GBP,
	Invoice_Source,
	INTL_Flag,
	Partner_Type,
	US_Partner_Type,
	Fastlane_Service_Type,
	Fastlane_Event_Type
FROM Dedicated
------------------------------------
UNION ALL
------------------------------------
SELECT
	Opportunity_Number,
	Account_Key,
	Account_number,
	Device_Number,
	Term,
	Final_Opportunity_Type,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	Close_Date,
	Account_Desired_Billing_Date,
	Account_Last_Billing_Date,
	Account_Status,
	Account_Create_Date,
	Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	Oracle_Vendor_ID,
	Tier_Level,
	Points,
	Program,
	Transaction_Type,
	Product_Group,
	Product_Type,
	Charge_Type		   AS Product_Type_Charge_Type,
	Invoice_Time_Month_Key,
	TOTAL,
	TOTAL_USD_GBP,
	Invoice_Source,
	INTL_Flag,
	Partner_Type,
	US_Partner_Type,
	Fastlane_Service_Type,
	Fastlane_Event_Type
FROM Email;

DELETE FROM `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE lower(Commissions_Role) like '%credit%' and lower(Product_Group) in ('rackspace email','managed exchange')
AND Invoice_Time_Month_Key = CurrentTime_Month
;
--------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE ( lower(Product_Type) in ('cloud site domain registration fee - 1 year','cloud site domain registration fee - 2 years','reserved space fee','loadbalancer bandwidth usage charge - aud','loadbalancer bandwidth usage charge - eur','cloud files bandwidth tiered usage charge','rackspace cdn bandwidth','one time fee','cloud files bandwidth cdn tiered usage charge','cloud load balancer bandwidth tiered usage charge split','uk cloud load balancer bandwidth tiered usage charge split','cloud site domain registration fee - 5 years','cloud sites bandwidth discount','server bandwidth out - aud','re one-time fee','uk server bandwidth out - usd','cloud site domain registration fee - 3 years','cloud legacy server bandwidth tiered usage charge','server bandwidth out - eur','cloud files bandwidth cdn usage charge - eur','cloud site domain registration fee - 4 years','uk cloud server bandwidth tiered usage charge','cloud files bandwidth cdn usage charge - aud','cloud files cdn bandwidth custom 02','uk cloud files bandwidth cdn tiered charge','cloud server bandwidth tiered usage charge','server bandwidth out - gbp')--,'system support fee fold','us legacy managed fee fold product') removed 3/28/16 per kl
OR Product_Group in (--'Professional Services',
'Adjustment','Domain Registration','Miscellaneous','Overage'))
AND Invoice_Time_Month_Key = CurrentTime_Month
;


end;