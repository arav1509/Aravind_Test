CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_all_line_item_detail()
begin
-----------------------------------------------------------------------------------------------------------------------------------------------------------
DECLARE StartDate int64;
SET StartDate = 201401;
-------------------------------------------------------------------------------------------------------------------
--if exists (select * from dbo.sysobjects where id = object_id(N'Partner_All_Line_Item_Detail') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
--drop table Partner_All_Line_Item_Detail
---------------------------------------------------------------------------------------------------------------
create or replace temp table Exchange as
SELECT
	Exchange_Rate_From_Currency_Code,
	Exchange_Rate_To_Currency_Code,
	Exchange_Rate_Time_Month_Key,
	Exchange_Rate_Exchange_Rate_Value
FROM
	`rax-abo-72-dev`.net_revenue.report_exchange_rate x
WHERE
	Source_system_Name = 'Oracle'
  and  upper(Exchange_Rate_To_Currency_Code) = 'GBP'
  and Exchange_Rate_Time_Month_Key >= StartDate
  ;
---------------------------------------------------------------------------------------------------------------
create or replace temp table Cloud as
SELECT
	Opportunity_Number,
	Cloud_Account_Key				as Account_Key,
	Account_number,
	Device_Number,
	Term,
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
	Time_Month_Key												AS Invoice_Time_Month_Key,
	Currency_Abbrev,
	SUM(TOTAL)													AS TOTAL,
	SUM(TOTAL)													AS TOTAL_USD_GBP,
	'US CLOUD'														AS Invoice_Source,
	Partner_Type,
	US_Partner_Type

FROM  `rax-abo-72-dev`.sales.partner_program_accounts_all2 A 
JOIN
	`rax-abo-72-dev`.slicehost.cloud_invoice_line_item_detail B  
ON A.Account_Number = cast(B.Account as string)
WHERE 
	lower(Account_Source) ='Cloud'
AND B.Time_Month_Key >=StartDate
AND Is_Net_Revenue = 1
GROUP BY
Opportunity_Number,
Cloud_Account_Key,
Account_Number,
Device_Number,
Term,
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
B.Charge_Type,
Time_Month_Key,
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
	Time_Month_Key										AS Invoice_Time_Month_Key,
	Currency_Abbrev,
	SUM(TOTAL)											AS TOTAL,
	SUM(TOTAL_USD)											AS TOTAL_USD_GBP,
	'UK CLOUD'												AS Invoice_Source,
	Partner_Type,
	US_Partner_Type
FROM `rax-abo-72-dev`.sales.partner_program_accounts_all2 A 
JOIN
	`rax-abo-72-dev`.cloud_uk.cloud_invoice_line_item_detail B  
ON A.Account_Number = cast(B.Account as string)
WHERE 
	lower(Account_Source) = 'cloud'
AND B.Time_Month_Key >=StartDate
AND Is_Net_Revenue = 1
GROUP BY
Opportunity_Number,
Cloud_Account_Key,
Account_Number,
Device_Number,
Term,
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
Time_Month_Key,
Currency_Abbrev,
Partner_Type,
	US_Partner_Type
	;
-------------------
UPDATE Cloud c
SET
	c.TOTAL_USD_GBP = A.TOTAL*B.Exchange_Rate_Exchange_Rate_Value
FROM	
	Cloud A
JOIN Exchange B
ON A.CURRENCY_ABBREV = B.Exchange_Rate_From_Currency_Code
AND a.Invoice_Time_Month_Key = B.Exchange_Rate_Time_Month_Key
WHERE 
	A.INTL_Flag = 1;
--------------------------------------------------------------------------------------------------------------
create or replace temp table Dedicated_xx as
SELECT
	Opportunity_Number,
	CONCAT(CAST(Account_Number as string),'','Dedicated')	 AS Account_Key,
	Account_number,
	Device_Number,
	Term,
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
	END																AS GL_Product_Hierarchy,
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
	Time_Month_Key												AS Invoice_Time_Month_Key,
	Currency,
	SUM(TOTAL)													AS TOTAL,
	SUM(TOTAL_USD)													AS TOTAL_USD_GBP,
	'Dedicated'														AS Invoice_Source,
	Partner_Type,
	US_Partner_Type

FROM `rax-abo-72-dev`.sales.partner_program_accounts_all2 A 
LEFT JOIN
	`rax-abo-72-dev`.net_revenue.dedicated_account_invoice_detail B  
ON A.Account_Number = B.Account
AND cast(A.Device_Number as string) = cast(ifnull(B.Server,'0') as string)
LEFT OUTER JOIN
    `rax-abo-72-dev`.net_revenue.dim_gl_product_mapping  DGP 
ON B.GL_Product_PK=DGP.GL_Product_PK
LEFT OUTER JOIN
   `rax-abo-72-dev`.net_revenue.dim_dedicated_0000_gl_products  DGP00 
ON B.GL_Product_PK = DGP00.GL_Product_PK
WHERE 
	lower(Account_Source) ='dedicated'
AND B.Time_Month_Key >=StartDate
AND Is_Net_Revenue = 1
--AND B.Product_Type NOT LIKE '%Prior%Period%' 
GROUP BY
	Opportunity_Number,
Concat(CAST(Account_Number as string),'','Dedicated'),
Account_Number,
Device_Number,
Term,
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
			DGP00.GL_Product_Billing_Source
		ELSE
			DGP.GL_Product_Billing_Source
	END,
B.Product_Group,
B.Product_Type,
B.GL_Account_Charge_Type,
Time_Month_Key,
Currency,
Partner_Type,
	US_Partner_Type
	;
--------------------------------------------------------------------------------------------------------------
create or replace temp table Dedicated as
SELECT
	Opportunity_Number,
	Account_Key,
	Account_number,
	Device_Number,
	Term,
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
	US_Partner_Type
FROM
	Dedicated_xx A
INNER JOIN
	`rax-abo-72-dev`.net_revenue.dim_dedicated_gl_codes  DGC 
ON A.GL_Account=DGC.GL_Code
WHERE
    ( lower(GL_Product_Hierarchy )in('dedicated','datapipe')
OR    lower(GL_Product_Description) in ('tricore','akamai'))
AND GL_Is_Dedicated_Revenue=1;

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
WHERE 
	A.INTL_Flag = 1;
---------------------------------------------------------------------------------------------------
create or replace temp table Email AS 
SELECT
	Opportunity_Number,
	CONCAT(CAST(Account_Number as string),'','Cloud_Email_Apps_Rack_Indirect')	 AS Account_Key,
	Account_number,
	Device_Number,
	Term,
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
	B.GL_Account_Charge_Type											AS Charge_Type,
	Time_Month_Key												AS Invoice_Time_Month_Key,
	Currency,
	SUM(TOTAL)													AS TOTAL,
	SUM(TOTAL_USD)													AS TOTAL_USD_GBP,
	'Email Indirect'												AS Invoice_Source,
	Partner_Type,
	US_Partner_Type

FROM 
	`rax-abo-72-dev`.sales.partner_program_accounts_all2 A 
LEFT JOIN
	`rax-abo-72-dev`.net_revenue.dedicated_account_invoice_detail B  
ON A.Account_Number = B.Account
AND cast(A.Device_Number as string) = cast(ifnull(B.Server,'0') as string)
INNER JOIN
   `rax-abo-72-dev`.net_revenue.dim_gl_products  dgp 
ON B.Oracle_Product_ID=DGP.GL_Product_Code
WHERE 
	lower(Account_Source) = 'Dedicated'
AND B.Time_Month_Key >=StartDate
AND Is_Net_Revenue = 1
--AND	Product_Type NOT LIKE '%Prior%Period%' 
AND lower(GL_Product_Hierarchy)='cloud apps'
AND B.GL_Account<>'120900'
GROUP BY
	Opportunity_Number,
concat(CAST(Account_Number as string),'','Cloud_Email_Apps_Rack_Indirect'),
Account_Number,
Device_Number,
Term,
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
B.GL_Account_Charge_Type,
Time_Month_Key,
Currency,
Partner_Type,
	US_Partner_Type
------------------------------------
UNION ALL
------------------------------------
SELECT
	Opportunity_Number,
	concat(CAST(Account as string) ,'','Cloud_Email_Apps' )	AS Account_Key,
	Account_number,
	Device_Number,
	Term,
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
	Time_Month_Key									AS Invoice_Time_Month_Key,
	Currency,
	SUM(TOTAL)										AS TOTAL,
	SUM(TOTAL)											AS TOTAL_USD_GBP,
	'Email SA3'											AS Invoice_Source,
	Partner_Type,
	US_Partner_Type
FROM 
	`rax-abo-72-dev`.sales.partner_program_accounts_all2 A 
LEFT JOIN
	`rax-abo-72-dev`.net_revenue.email_apps_account_invoice_detail B 
ON A.Account_Number= B.Account
WHERE
	lower(Account_Source) = 'email'
AND lower(program) = 'cloud partner'
AND	Include_in_payable=1
AND CURRENCY = 'USD'
AND B.Time_Month_Key >=StartDate
GROUP BY
	Opportunity_Number,
concat(CAST(Account as string) ,'','Cloud_Email_Apps' ),
Account_Number,
Device_Number,
Term,
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
Time_Month_Key,
Currency,
Partner_Type,
	US_Partner_Type;
--------------------------------------------------------------------------------------------------------------
UPDATE  Email e
SET
	e.TOTAL_USD_GBP = A.TOTAL*B.Exchange_Rate_Exchange_Rate_Value
FROM	
	Email A
JOIN
	Exchange B
ON A.CURRENCY = B.Exchange_Rate_From_Currency_Code
AND a.Invoice_Time_Month_Key = B.Exchange_Rate_Time_Month_Key
WHERE 
	A.INTL_Flag = 1;
----------------------------------------------------------------------------------------------------------------
create or replace table `rax-abo-72-dev`.sales.partner_all_line_item_detail AS
SELECT
	Opportunity_Number,
	Account_Key,
	Account_number,
	Device_Number,
	Term,
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
	US_Partner_Type
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
	US_Partner_Type
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
	US_Partner_Type
FROM Email;

END;
