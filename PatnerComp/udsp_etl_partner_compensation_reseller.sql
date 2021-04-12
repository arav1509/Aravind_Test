CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_compensation_reseller(V_Date date)

begin
-------------------------------------------------------------------------------------------------------------------
DECLARE CurrentMonthYear datetime;
DECLARE CurrentTime_Month int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;
-------------------------------------------------------------------------------------------------------------------
--T CurrentMonthYear=V_Date;
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);
-------------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_compensation_reseller WHERE Invoice_Time_Month_Key=CurrentTime_Month;
-------------------------------------------------------------------------------------------------------------------
create or replace temp table Partner_Program_Line_Item_Detail_Temp as 
SELECT	* --INTO Partner_Program_Line_Item_Detail_Temp
FROM `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE 
	Invoice_Time_Month_Key = CurrentTime_Month;
------------------------------------------------------------------------------------------------------------------
DELETE FROM  Partner_Program_Line_Item_Detail_Temp t
where STRUCT(t.Product_group,t.Product_Type)
in (
	select STRUCT(A.Product_group,A.Product_Type)
  from
	Partner_Program_Line_Item_Detail_Temp A
	JOIN `rax-abo-72-dev`.sales.dim_partner_nontraditional_products B
	ON A.Product_group = B.Product_Group
	AND A.Product_Type = B.Product_Type
	WHERE	lower(B.Partner_Comp_Type) = 'colo'
);
------------------------------------------------------------------------------------------------------------------
create or replace  temp table Opportunity  as
SELECT 
	Cast(0 as string) as Account_Number,
	Partner_Account,
	'N/A' as Partner_Role,
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
	'N/A' Commissions_Role,
	Oracle_Vendor_ID,
	Tier_Level,
	Points,
	INTL_Flag,
	Invoice_Time_Month_Key,
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP
FROM 
	Partner_Program_Line_Item_Detail_Temp 
WHERE
	((lower(partner_contract_type) like '%reseller%' and lower(partner_contract_type) not like '%/%' and lower(commissions_role) like '%credit%')
or (lower(partner_contract_type) like '%referral/reseller%' and lower(commissions_role) like '%credit%'))
and lower(transaction_type) = 'inv'
and lower(product_group ) not in ('rackspace email','managed exchange')
AND Term>=12
AND Invoice_Time_Month_Key =CurrentTime_Month
--AND TOTAL>=0
GROUP BY
	Partner_Account,
	--Partner_Role,
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
	--Commissions_Role,
	Oracle_Vendor_ID,
	Tier_Level,
	Points,
	INTL_Flag,
	Invoice_Time_Month_Key
	;
----------------------------------------
create or replace temp table Comm_Temp as
SELECT
	Account_Number,
	Partner_Account,
	Partner_Role,
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
	INTL_Flag,
	Invoice_Time_Month_Key,
	Total,
	Total_USD_GBP,
	Comm											AS Comm_Percentage,
	Max_Comm+((TOTAL_USD_GBP-Tier_Start)*Comm)			AS Comm
FROM Opportunity
LEFT JOIN
	`rax-abo-72-dev`.sales.partner_compensation_commission_tiers
ON TOTAL_USD_GBP BETWEEN Tier_Start and Tier_End
AND lower(Type) = 'Reseller'
;
-------------------------------------------------------------------------------------------------------------------
create or replace temp table Partner_Compensation_Reseller_Temp as
SELECT
	Account_Number,
	Partner_Account,
	Partner_Role,
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
	INTL_Flag,
	Invoice_Time_Month_Key,
	Total,
	Total_USD_GBP,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End as Comm_Type
FROM	Comm_Temp;
-------------------------------------------------------------------------------
create or replace temp table Account as
SELECT
	Account_Number,
	Partner_Account,
	Invoice_Time_Month_Key,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE
	((lower(partner_contract_type) like '%reseller%' and lower(partner_contract_type) not like '%/%' and lower(commissions_role) like '%credit%')
or (lower(partner_contract_type) like '%referral/reseller%' and lower(commissions_role) like '%credit%'))
and lower(transaction_type) = 'inv'
and lower(product_group ) not in ('rackspace email','managed exchange')
AND Term>=12
AND Invoice_Time_Month_Key =CurrentTime_Month
GROUP BY
	Account_Number,
	Partner_Account,
	Invoice_Time_Month_Key;
-----------------------------------------------------------------
create or replace temp table Account2 as
SELECT
	Partner_Account,
	MAX(Total_USD_GBP) as Max_Total
FROM Account
GROUP BY
	Partner_Account;
--------------------------------------------------------------------
create or replace temp table Account_Final as 
SELECT DISTINCT
	Account_Number,
	A.Partner_Account
FROM Account A
JOIN Account2 B
ON A.Partner_Account = B.Partner_Account
and A.Total_USD_GBP = B.Max_Total;
----------------------------------------------------------------------
UPDATE Partner_Compensation_Reseller_Temp t
SET
t.Account_Number = B.Account_Number
FROM
	Partner_Compensation_Reseller_Temp A 
JOIN Account_Final B
ON A.Partner_Account = B.Partner_Account
where true;
----------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_reseller
SELECT
	Account_Number,
	Partner_Account,
	Partner_Role,
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
	Invoice_Time_Month_Key,
	Total,
	Total_USD_GBP,
	Comm_Percentage,
	Comm,
	Comm_Type,
	INTL_Flag
	
FROM Partner_Compensation_Reseller_Temp;
---------------------------------------------------------------------------

end;
