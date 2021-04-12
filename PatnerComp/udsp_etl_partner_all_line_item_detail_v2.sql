CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_all_line_item_detail_v2()
-------------------------------------------------------------------------------------------------------------------
--@Date datetime

/*

03/11/2019		CHANDRA PUTTA	Corrected the column name Line_of_Businesss
*/
begin
-----------------------------------------------------------------------------------------------------------------------------------------------------------
DECLARE StartDate int64;
SET StartDate = 201401;
-------------------------------------------------------------------------------------------------------------------
--if exists (select * from dbo.sysobjects where id = object_id(N'Partner_All_Line_Item_Detail_v2') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
--drop table Partner_All_Line_Item_Detail_v2
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
	   lower(Source_system_Name) = 'oracle'
  and  lower(Exchange_Rate_To_Currency_Code) = 'gbp'
  and Exchange_Rate_Time_Month_Key >= StartDate;


create or replace temp table Exchange_USD as
  SELECT
	Exchange_Rate_From_Currency_Code,
	Exchange_Rate_To_Currency_Code,
	Exchange_Rate_Time_Month_Key,
	Exchange_Rate_Exchange_Rate_Value

FROM
	`rax-abo-72-dev`.net_revenue.report_exchange_rate x
where
	   lower(Source_system_Name) = 'oracle'
  and  lower(Exchange_Rate_To_Currency_Code) = 'usd'
  and  Exchange_Rate_Time_Month_Key >= StartDate
  ;
---------------------------------------------------------------------------------------------------------------
create or replace temp table Cloud as
SELECT
	Opportunity_Number,
	Cloud_Account_Key				as Account_Key,
	Account_number,
	Account_Name,
	'CLOUD'										AS Line_of_Business,
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
	Currency_Abbrev as Currency,
	SUM(TOTAL)													AS TOTAL,
	SUM(TOTAL_USD)													AS TOTAL_USD,
	SUM(TOTAL)													AS TOTAL_USD_GBP,
	'US CLOUD'														AS Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested

FROM 
	`rax-abo-72-dev`.sales.partner_program_accounts_all3 A
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
Account_Name,
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
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested
------------------------------------
UNION ALL
------------------------------------
SELECT
	Opportunity_Number,
	Cloud_Account_Key				as Account_Key,
	Account_number,
	Account_Name,
	'CLOUD UK'									AS Line_of_Business,
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
	SUM(TOTAL_USD)													AS TOTAL_USD,
	SUM(TOTAL_USD)											AS TOTAL_USD_GBP,
	'UK CLOUD'												AS Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested
FROM 
	`rax-abo-72-dev`.sales.partner_program_accounts_all3 A
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
Account_Name,
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
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested
	;
-------------------
UPDATE   Cloud c
SET
	c.TOTAL_USD_GBP = A.TOTAL*B.Exchange_Rate_Exchange_Rate_Value
FROM	
	Cloud A
JOIN
	Exchange B
ON A.Currency = B.Exchange_Rate_From_Currency_Code
AND a.Invoice_Time_Month_Key = B.Exchange_Rate_Time_Month_Key
WHERE 
	A.INTL_Flag = 1;
--------------------------------------------------------------------------------------------------------------
create or replace temp table Dedicated_xx as
SELECT
	Opportunity_Number,
	concat(CAST(Account_Number as string),'','Dedicated' )	 AS Account_Key,
	Account_number,
	Account_Name,
	Line_of_Business,
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
	SUM(TOTAL_USD)													AS TOTAL_USD,
	SUM(TOTAL_USD)													AS TOTAL_USD_GBP,
	'Dedicated'														AS Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested
FROM `rax-abo-72-dev`.sales.partner_program_accounts_all3 A
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
concat(CAST(Account_Number as string),'','Dedicated' ),
Account_Number,
Account_Name,
Line_of_Business,
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
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested
	;
--------------------------------------------------------------------------------------------------------------
create or replace temp table  Dedicated as 
SELECT
	Opportunity_Number,
	Account_Key,
	Account_number,
	Account_Name,
	Line_of_Business,
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
	TOTAL_USD,
	TOTAL_USD_GBP,
	Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested
FROM Dedicated_xx A
INNER JOIN
	`rax-abo-72-dev`.net_revenue.dim_dedicated_gl_codes  DGC
ON A.GL_Account=DGC.GL_Code
WHERE
   (lower(GL_Product_Hierarchy) in('dedicated','datapipe')
OR  lower(GL_Product_Description) IN ('tricore','akamai'))
AND GL_Is_Dedicated_Revenue=1
;
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
create or replace temp table Datapipe as
SELECT
'N/A' as Opportunity_Number,
B.Account_Key,
B.Account_Number,
B.Account_Name,
B.Line_Of_Business,
B.Device_Number,
0 as Term,
Is_Cloud_legally_Linked as Is_Linked_Account,
Is_Cloud_Consolidated as Is_Consolidated_Billing,
Internal_Flag as Is_Internal_Account,
'1/1/9999' as Close_Date,
'1/1/9999' as Account_Desired_Billing_Date,
'1/1/9999' as Account_Last_Billing_Date,
Account_Status,
'1/1/9999' as Account_Create_Date,
'1/1/9999' as Account_End_Date,
0 as Account_Tenure,
A.Partner_Account,
A.Partner_Role,
CASE WHEN      lower(Commissions_Role) like 'pay co%' 
			or lower(Commissions_Role) like '%credit%' 
		 THEN 1 
		 WHEN lower(Partner_Contract_Type) in ('aus reseller agreement','us reseller agreement','emea reseller agreement','apac reseller agreement','hk reseller agreement','latam reseller agreement') 
			and lower(Commissions_Role) not like '%pay co%' 
		 THEN 1 ELSE 0 END						AS Pay_Commissions,
A.Partner_Account_Name,
A.Partner_Account_Type,
A.Partner_Account_Sub_Type,
'1/1/9999' as Partner_Contract_Signed_Date,
'N/A' as Partner_Account_RSA_ID,
'N/A' as Partner_Account_RV_EXT_ID,
'RV' as Partner_Account_RSA_or_RV,
A.Partner_Account_Owner,
A.Partner_Account_Owner_Role,
A.Partner_Contract_Type,
A.Commissions_Role,
cast(A.Oracle_Vendor_ID as string) as Oracle_Vendor_ID,
A.Tier_Level,
'N/A' as Points,
'Datapipe' as Program,
B.Transaction_Type,
B.Product_Group,
B.Product_Type,
'N/A' as Product_Type_Charge_Type,
B.Time_Month_Key as Invoice_Time_Month_Key,
B.Currency,
Local_Total_Invoiced as Total,
Total_Invoiced as Total_USD,
Local_Total_Invoiced as Total_USD_GBP,
B.Invoice_Line_Item_Source as Invoice_Source,
0 as INTL_Flag,
A.Partner_Type,
'N/A' as US_Partner_Type,
'N/A' as PartnerAssociated,
'N/A' as	Partner_Divested

FROM
	`rax-abo-72-dev`.sales.partner_accounts_datapipe A
JOIN
	`rax-abo-72-dev`.net_revenue.net_revenue_detail B
ON cast(A.Datapipe_ID as string) = B.Account_Number
WHERE
	B.Time_Month_Key >=StartDate
AND lower(Line_Of_Business) = 'datapipe';
---------------------------------------------------------------------------------------------------
UPDATE  Datapipe D
SET
	D.TOTAL_USD_GBP = A.TOTAL*B.Exchange_Rate_Exchange_Rate_Value
FROM	
	Datapipe A
JOIN
	Exchange B
ON A.CURRENCY = B.Exchange_Rate_From_Currency_Code
AND a.Invoice_Time_Month_Key = B.Exchange_Rate_Time_Month_Key
WHERE 
	upper(A.Currency) not in ('USD','GBP')
	;
---------------------------------------------------------------------------------------------------
create or replace temp table Email as
SELECT
	Opportunity_Number,
	concat(CAST(Account_Number as string),'','Cloud_Email_Apps_Rack_Indirect' ) AS Account_Key,
	Account_number,
	Account_Name,
	Line_of_Business,
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
	SUM(TOTAL_USD)													AS TOTAL_USD,
	SUM(TOTAL_USD)													AS TOTAL_USD_GBP,
	'Email Indirect'												AS Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested
FROM `rax-abo-72-dev`.sales.partner_program_accounts_all3 A
LEFT JOIN
	`rax-abo-72-dev`.net_revenue.dedicated_account_invoice_detail B 
ON A.Account_Number = B.Account
AND cast(A.Device_Number as string) = cast(ifnull(B.Server,'0') as string)
INNER JOIN
   `rax-abo-72-dev`.net_revenue.dim_gl_products  DGP
ON B.Oracle_Product_ID=DGP.GL_Product_Code
WHERE 
	lower(Account_Source) = 'dedicated'
AND B.Time_Month_Key >=StartDate
AND Is_Net_Revenue = 1
--AND	Product_Type NOT LIKE '%Prior%Period%' 
AND lower(GL_Product_Hierarchy)='cloud apps'
AND B.GL_Account<>'120900'
GROUP BY
	Opportunity_Number,
concat(CAST(Account_Number as string),'','Cloud_Email_Apps_Rack_Indirect' ),
Account_Number,
Account_Name,
Line_of_Business,
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
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested
------------------------------------
UNION ALL
------------------------------------
SELECT
	Opportunity_Number,
	concat(CAST(Account as string) ,'','Cloud_Email_Apps' )	AS Account_Key,
	Account_number,
	A.Account_Name,
	'Email&Apps' AS Line_of_Business,
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
	SUM(TOTAL)													AS TOTAL_USD,
	SUM(TOTAL)											AS TOTAL_USD_GBP,
	'Email SA3'											AS Invoice_Source,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested
FROM `rax-abo-72-dev`.sales.partner_program_accounts_all3 A
LEFT JOIN
	 `rax-abo-72-dev`.net_revenue.email_apps_account_invoice_detail B 
ON A.Account_Number= B.Account
WHERE
	lower(Account_SOURCE) = 'email'
AND lower(Program) = 'cloud partner'
AND	Include_in_payable=1
AND lower(CURRENCY) = 'usd'
AND B.Time_Month_Key >=StartDate
GROUP BY
	Opportunity_Number,
concat(CAST(Account  as string),'','Cloud_Email_Apps'),
Account_Number,
A.Account_Name,
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
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested;
--------------------------------------------------------------------------------------------------------------
UPDATE  Email E
SET
	E.TOTAL_USD_GBP = A.TOTAL*B.Exchange_Rate_Exchange_Rate_Value
FROM	
	Email A
JOIN
	Exchange B
ON A.CURRENCY = B.Exchange_Rate_From_Currency_Code
AND a.Invoice_Time_Month_Key = B.Exchange_Rate_Time_Month_Key
WHERE 
	A.INTL_Flag = 1;
--------------------------------------------------------------------
UPDATE  Email E
SET
	E.TOTAL_USD = A.TOTAL*B.Exchange_Rate_Exchange_Rate_Value
FROM	
	Email A
JOIN
	Exchange B
ON A.CURRENCY = B.Exchange_Rate_From_Currency_Code
AND a.Invoice_Time_Month_Key = B.Exchange_Rate_Time_Month_Key
WHERE 
	upper(A.Currency) <> 'USD';
----------------------------------------------------------------------------------------------------------------
create or replace table `rax-abo-72-dev`.sales.partner_all_line_item_detail_v2 as
SELECT
	Opportunity_Number,
	Account_Key,
	Account_number,
	Account_Name,
	Line_of_Business,
	Device_Number,
	Term,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	CAST(Close_Date AS DATETIME) AS Close_Date,
	CAST(Account_Desired_Billing_Date AS DATETIME) AS Account_Desired_Billing_Date,
	CAST(Account_Last_Billing_Date AS DATETIME) AS Account_Last_Billing_Date,
	Account_Status,
	CAST(Account_Create_Date AS DATETIME)  AS Account_Create_Date,
	CAST(Account_End_Date  AS DATETIME)  AS Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	CAST(Partner_Contract_Signed_Date AS DATETIME ) AS Partner_Contract_Signed_Date,
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
	Currency,
	TOTAL,
	TOTAL_USD,
	TOTAL_USD_GBP,
	Invoice_Source,
	INTL_Flag,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested,
	'Traditional' as Compensation_Type
FROM Cloud
------------------------------------
UNION ALL
------------------------------------
SELECT
	Opportunity_Number,
	Account_Key,
	Account_number,
	Account_Name,
	Line_of_Business,
	Device_Number,
	Term,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	CAST(Close_Date AS DATETIME) AS Close_Date,
	CAST(Account_Desired_Billing_Date AS DATETIME) AS Account_Desired_Billing_Date,
	CAST(Account_Last_Billing_Date AS DATETIME) AS Account_Last_Billing_Date,
	Account_Status,
	CAST(Account_Create_Date AS DATETIME)  AS Account_Create_Date,
	CAST(Account_End_Date  AS DATETIME)  AS Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	CAST(Partner_Contract_Signed_Date AS DATETIME ) AS Partner_Contract_Signed_Date,
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
	Currency,
	TOTAL,
	TOTAL_USD,
	TOTAL_USD_GBP,
	Invoice_Source,
	INTL_Flag,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested,
	'Traditional' as Compensation_Type
FROM Dedicated
------------------------------------
UNION ALL
------------------------------------
SELECT
	Opportunity_Number,
	Account_Key,
	Account_number,
	Account_Name,
	Line_of_Business,
	Device_Number,
	Term,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	CAST(Close_Date AS DATETIME) AS Close_Date,
	CAST(Account_Desired_Billing_Date AS DATETIME) AS Account_Desired_Billing_Date,
	CAST(Account_Last_Billing_Date AS DATETIME) AS Account_Last_Billing_Date,
	Account_Status,
	CAST(Account_Create_Date AS DATETIME)  AS Account_Create_Date,
	CAST(Account_End_Date  AS DATETIME)  AS Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	CAST(Partner_Contract_Signed_Date AS DATETIME ) AS Partner_Contract_Signed_Date,
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
	Currency,
	TOTAL,
	TOTAL_USD,
	TOTAL_USD_GBP,
	Invoice_Source,
	INTL_Flag,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested,
	'Traditional' as Compensation_Type
FROM Email
-----------------------
UNION ALL
-----------------------
SELECT
	Opportunity_Number,
	Account_Key,
	Account_number,
	Account_Name,
	Line_of_Business,
	Device_Number,
	Term,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	CAST(Close_Date AS DATETIME) AS Close_Date,
	CAST(Account_Desired_Billing_Date AS DATETIME) AS Account_Desired_Billing_Date,
	CAST(Account_Last_Billing_Date AS DATETIME) AS Account_Last_Billing_Date,
	Account_Status,
	CAST(Account_Create_Date AS DATETIME)  AS Account_Create_Date,
	CAST(Account_End_Date  AS DATETIME)  AS Account_End_Date,
	Account_Tenure,
	Partner_Account,
	Partner_Role,
	Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	CAST(Partner_Contract_Signed_Date AS DATETIME ) AS Partner_Contract_Signed_Date,
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
	Product_Type_Charge_Type,
	Invoice_Time_Month_Key,
	Currency,
	TOTAL,
	TOTAL_USD,
	TOTAL_USD_GBP,
	Invoice_Source,
	INTL_Flag,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested,
	'Datapipe' as Compensation_Type
FROM Datapipe;
--------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_all_line_item_detail_v2
WHERE lower(Commissions_Role) like '%credit%' and lower(Product_Group) in ('rackspace email','managed exchange');
--------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_all_line_item_detail_v2
WHERE Product_Type in ('Cloud Site Domain Registration Fee - 1 Year','Cloud Site Domain Registration Fee - 2 Years','Reserved Space Fee','LoadBalancer Bandwidth Usage Charge - AUD','LoadBalancer Bandwidth Usage Charge - EUR','Cloud Files Bandwidth Tiered Usage Charge','Rackspace CDN Bandwidth','One Time Fee','Cloud Files Bandwidth CDN Tiered Usage Charge','Cloud Load Balancer Bandwidth Tiered Usage Charge Split','UK Cloud Load Balancer Bandwidth Tiered Usage Charge Split','Cloud Site Domain Registration Fee - 5 Years','Cloud Sites Bandwidth Discount','Server Bandwidth Out - AUD','RE One-Time Fee','UK Server Bandwidth Out - USD','Cloud Site Domain Registration Fee - 3 Years','Cloud Legacy Server Bandwidth Tiered Usage Charge','Server Bandwidth Out - EUR','Cloud Files Bandwidth CDN Usage Charge - EUR','Cloud Site Domain Registration Fee - 4 Years','UK Cloud Server Bandwidth Tiered Usage Charge','Cloud Files Bandwidth CDN Usage Charge - AUD','Cloud Files CDN Bandwidth Custom 02','UK Cloud Files Bandwidth CDN Tiered Charge','Cloud Server Bandwidth Tiered Usage Charge','Server Bandwidth Out - GBP')--,'System Support Fee Fold','US Legacy Managed Fee Fold Product') Removed 3/28/16 per KL
OR lower(Product_Group) in ('aggregate bandwidth','adjustment','domain registration','miscellaneous','overage');

end;
