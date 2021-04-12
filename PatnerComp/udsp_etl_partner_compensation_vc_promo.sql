CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_compensation_vc_promo(V_Date date)
begin
--------------------------------------------------------------------------------------------------------------------
DECLARE CurrentMonthYear  datetime;
DECLARE v_O365  datetime;
DECLARE CurrentTime_Month  int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;
-------------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=V_Date;
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);
SET v_O365 = '2016-07-19';
-------------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_compensation_vc_promo WHERE Invoice_Time_Month_Key=CurrentTime_Month;
-------------------------------------------------------------------------------------------------------------------
create or replace temp table Opportunity  as 
SELECT 
	Opportunity_Number,
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
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP
FROM `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
--AND C.Partner_Comp = 1
WHERE 
	lower(Partner_Contract_Type) like '%vc promo%'
AND lower(Transaction_Type) = 'inv'
--AND TOTAL >=0
AND Invoice_Time_Month_Key = CurrentTime_Month
AND C.Product_Group is null
GROUP BY
	Opportunity_Number,
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
	Invoice_Time_Month_Key
union all
SELECT 
	Opportunity_Number,
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
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP
	
FROM `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE lower(Partner_Contract_Type) like '%vc promo%'
AND lower(Transaction_Type) = 'inv'
--AND TOTAL >=0
AND Invoice_Time_Month_Key = CurrentTime_Month
AND C.Partner_Comp_Type = 'O365'
AND Close_Date < v_O365

GROUP BY
	Opportunity_Number,
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
	Invoice_Time_Month_Key;
----------------------------------------
create or replace temp table v_Comm as 
SELECT
	Opportunity_Number,
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
	.15 AS Comm_Percentage,
	TOTAL_USD_GBP*.15 AS Comm
FROM Opportunity A;
-------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_vc_promo
SELECT
	Opportunity_Number,
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
	cast(Comm_Percentage as NUMERIC ) as Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%commissions%' then 'Commissions'
		 End																as Comm_Type,
	'Traditional'
 
FROM v_Comm;
-------------------------------------------------------------------------------
--AWS
create or replace temp table Opportunity2  as
SELECT 
	Opportunity_Number,
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
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP
FROM 
	`rax-abo-72-dev`.sales.partner_program_line_item_detail A 
JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE 
	lower(Partner_Contract_Type) like '%vc promo%'
AND lower(Transaction_Type) = 'inv'
--AND TOTAL >=0
AND Invoice_Time_Month_Key = CurrentTime_Month
AND lower(C.Partner_Comp_Type) = 'aws'
GROUP BY
	Opportunity_Number,
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
	Invoice_Time_Month_Key;
----------------------------------------
create or replace temp table v_Comm2 as
SELECT
	Opportunity_Number,
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
	.10													AS Comm_Percentage,
	TOTAL_USD_GBP*.10										AS Comm
FROM Opportunity2 A;
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_vc_promo
SELECT
	Opportunity_Number,
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
	cast(Comm_Percentage as NUMERIC ) as Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%commissions%' then 'Commissions'
		 End																as Comm_Type,
	'AWS'
 
FROM v_Comm2;
-------------------------------------------------------------------------------------------------
--Azure
create or replace temp table Opportunity3 as
SELECT 
	Opportunity_Number,
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
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP 
FROM `rax-abo-72-dev`.sales.partner_program_line_item_detail  A 
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE 
	lower(Partner_Contract_Type) like '%vc promo%'
AND lower(Transaction_Type) = 'inv'
--AND TOTAL >=0
AND Invoice_Time_Month_Key = CurrentTime_Month
AND lower(C.Partner_Comp_Type)= 'azure'

GROUP BY
	Opportunity_Number,
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
	Invoice_Time_Month_Key;
----------------------------------------
create or replace temp table v_Comm3 as 
SELECT
	Opportunity_Number,
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
	.10													AS Comm_Percentage,
	TOTAL_USD_GBP*.10										AS Comm
FROM Opportunity3 A;
-------------------------------------------------------------------------------------------------------------------

INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_vc_promo
SELECT
	Opportunity_Number,
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
	cast(Comm_Percentage as NUMERIC ) as Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%commissions%' then 'Commissions'
		 End																as Comm_Type,
	'Azure'
FROM  v_Comm3;
--------------------------------------------------------------
--O365
create or replace temp table Opportunity4 as 
SELECT 
	Opportunity_Number,
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
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP
	

FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE 
lower(Partner_Contract_Type) like '%vc promo%'
AND lower(Transaction_Type) = 'inv'
--AND TOTAL >=0
AND Invoice_Time_Month_Key = CurrentTime_Month
AND C.Partner_Comp_Type = 'O365'
AND Close_Date >= v_O365

GROUP BY
	Opportunity_Number,
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
	Invoice_Time_Month_Key;
----------------------------------------
create or replace temp table v_Comm4 as 
SELECT
	Opportunity_Number,
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
	.10													AS Comm_Percentage,
	TOTAL_USD_GBP*.10										AS Comm
FROM Opportunity4 A;
-------------------------------------------------------------------------------------------------------------------

INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_vc_promo
SELECT
	Opportunity_Number,
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
	cast(Comm_Percentage as NUMERIC ) as Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%commissions%' then 'Commissions'
		 End																as Comm_Type,
	'O365'
 
FROM v_Comm4;
--------------------------------------------------------------
--Google
create or replace temp table Opportunity5 as 
SELECT 
	Opportunity_Number,
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
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP
FROM `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE 
	lower(Partner_Contract_Type) like '%vc promo%'
AND lower(Transaction_Type) = 'inv'
--AND TOTAL >=0
AND Invoice_Time_Month_Key = CurrentTime_Month
AND lower(C.Partner_Comp_Type) = 'Google'
--AND Close_Date >= v_O365

GROUP BY
	Opportunity_Number,
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
	Invoice_Time_Month_Key;
----------------------------------------
create or replace temp table  v_Comm5 as
SELECT
	Opportunity_Number,
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
	.10													AS Comm_Percentage,
	TOTAL_USD_GBP*.10										AS Comm
FROM Opportunity5 A;
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_vc_promo
SELECT
	Opportunity_Number,
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
	cast(Comm_Percentage as NUMERIC ) as Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%commissions%' then 'Commissions'
		 End																as Comm_Type,
	'Google'
 
FROM
	v_Comm5;
--------------------------------------------------------------
END;