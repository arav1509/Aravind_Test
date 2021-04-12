CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_compensation_referral(V_Date date)

begin
DECLARE CurrentMonthYear  datetime;
DECLARE v_O365  date;
DECLARE CurrentTime_Month int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;
-----------------------------------------------------------------------------------------------------------------------------------------------------------
--SET CurrentMonthYear=V_Date;
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);
SET v_O365 = cast('2016-07-19' as date);
-----------------------------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_compensation_referral WHERE Invoice_Time_Month_Key=CurrentTime_Month;
-----------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace temp table Referral as 
SELECT 
	Account_Key,
	cast(Account as string) as Account,
	Time_Month_Key,
	cast(Invoice_Ordinal as string) as Invoice_Ordinal
FROM  `rax-abo-72-dev`.slicehost.cloud_us_invoiced_w_ordinal 
WHERE 	Invoice_Ordinal in (1,2,3)
---------------------------------
UNION ALL
---------------------------------
SELECT 
	Account_Key,
	cast(Account as string) as Account,	
	Time_Month_Key,
	cast(Invoice_Ordinal as string) as Invoice_Ordinal

FROM `rax-abo-72-dev`.cloud_uk.cloud_uk_invoiced_w_ordinal 
WHERE Invoice_Ordinal in (1,2,3)
---------------------------------
UNION ALL
---------------------------------
SELECT 
	Account_Key,
	cast(Account as string) as Account,
	Time_Month_Key,
	cast(Invoice_Ordinal as string)

FROM `rax-abo-72-dev`.net_revenue.dedicated_invoice_ordinal
WHERE Invoice_Ordinal in (1,2,3)
---------------------------------
UNION ALL
---------------------------------
SELECT 
	Account_Key,
	cast(Account as string) as Account,
	Time_Month_Key,
	cast(Invoice_Ordinal as string)

FROM `rax-abo-72-dev`.net_revenue.cloud_email_apps_invoice_ordinal 
WHERE 	Invoice_Ordinal in (1,2,3)
	;
-------------------------------------------------------------------------
create or replace temp table TMK as
SELECT * --INTO #TMK
FROM Referral
WHERE 
	Account_Key in (SELECT Account_KEY FROM Referral WHERE Invoice_Ordinal = '3' and Time_Month_Key = CurrentTime_Month)
  ;
-------------------------------------------------------------------------
--Traditional
create or replace temp table Opportunity as
SELECT 
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	SUM(TOTAL)								AS TOTAL,
	SUM(TOTAL_USD_GBP)							AS TOTAL_USD_GBP,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END					AS Invoice_Type 
FROM 
	`rax-abo-72-dev`.sales.partner_program_line_item_detail A 
JOIN
	(
		SELECT * --INTO #TMK
		FROM Referral
		WHERE 	Account_Key in (SELECT Account_KEY FROM Referral WHERE Invoice_Ordinal = '3' and Time_Month_Key = CurrentTime_Month)
	) B --#TMK B
ON A.Account_Key = B.Account_Key
AND A.Invoice_Time_Month_Key = B.Time_Month_Key
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
--AND C.Partner_Comp = 1
WHERE
	(( lower(partner_contract_type) like '%referral%' and lower(partner_contract_type) not like '%/%' and lower(commissions_role) like '%commissions%')
or (lower(partner_contract_type) like '%referral/reseller%' and lower(commissions_role) like '%commissions%'))
and lower(partner_contract_type) not like '%strategic%'
AND lower(Transaction_Type) = 'inv'
AND Term>=12
AND C.Product_Group is null
--AND TOTAL>=0
GROUP BY
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END	
		 ;
----------------------------------------
INSERT INTO Opportunity
SELECT 
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	SUM(TOTAL)								AS TOTAL,
	SUM(TOTAL_USD_GBP)							AS TOTAL_USD_GBP,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END					AS Invoice_Type
	
FROM `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
JOIN
	(
		SELECT * --INTO #TMK
		FROM Referral
		WHERE 	Account_Key in (SELECT Account_KEY FROM Referral WHERE Invoice_Ordinal = '3' and Time_Month_Key = CurrentTime_Month)
	) B --#TMK B
ON A.Account_Key = B.Account_Key
AND A.Invoice_Time_Month_Key = B.Time_Month_Key
JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE
	((lower(partner_contract_type) like '%referral%' and lower(partner_contract_type) not like '%/%' and lower(commissions_role) like '%commissions%')
or (lower(partner_contract_type) like '%referral/reseller%' and lower(commissions_role) like '%commissions%'))
and lower(partner_contract_type) not like '%strategic%'
AND lower(Transaction_Type) = 'inv'
AND Term>=12
AND C.Partner_Comp_Type = 'O365'
AND Close_Date < v_O365
GROUP BY
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END	
					;
----------------------------------------
create or replace temp table AVG_Temp as
SELECT
	Opportunity_Number,
	Term,
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
	AVG(TOTAL)								AS Three_Mo_Average,
	AVG(TOTAL_USD_GBP)							AS Three_Mo_Average_USD_GBP,
	Invoice_Type
FROM Opportunity
WHERE
	TOTAL <> 0
GROUP BY
	Opportunity_Number,
Term,
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
Invoice_Type
;
----------------------------------------
create or replace temp table Comm_temp as
SELECT
	Opportunity_Number,
	Term,
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
	CurrentTime_Month													AS Invoice_Time_Month_Key,
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	CASE WHEN lower(Invoice_Type) = 'dedicated' AND Term >= 12 AND Term <24
		 THEN 1
		 WHEN lower(Invoice_Type) = 'dedicated' AND Term >= 24 AND Term <36
		 THEN 1.5
		 WHEN lower(Invoice_Type) = 'dedicated' AND Term >= 36
		 THEN 2
		 WHEN lower(Invoice_Type) = 'cloud/email'
		 THEN 1
		 END AS Comm_Payout,
	CASE WHEN lower(Invoice_Type) = 'dedicated' AND Term >= 12 AND Term <24
		 THEN Three_Mo_Average_USD_GBP
		 WHEN lower(Invoice_Type) = 'dedicated' AND Term >= 24 AND Term <36
		 THEN 1.5*Three_Mo_Average_USD_GBP
		 WHEN lower(Invoice_Type) = 'dedicated' AND Term >= 36
		 THEN 2*Three_Mo_Average_USD_GBP
		 WHEN lower(Invoice_Type) = 'cloud/email'
		 THEN Three_Mo_Average_USD_GBP
		 END AS Comm
FROM AVG_Temp
WHERE 	Three_Mo_Average_USD_GBP >=50;
-------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_referral
--create or replace table   `rax-abo-72-dev`.sales.partner_compensation_referral as
SELECT
	Opportunity_Number,
	Term,
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
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	cast(Comm_Payout as NUMERIC ) as Comm_Payout,
	Comm as Comm ,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'Traditional' as compensation_category

FROM Comm_temp;

-------------------------------------------------------------------------------
--AWS
create or replace temp table Opportunity2  as
SELECT 
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	SUM(TOTAL)								AS TOTAL,
	SUM(TOTAL_USD_GBP)							AS TOTAL_USD_GBP,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END					AS Invoice_Type
FROM 
	`rax-abo-72-dev`.sales.partner_program_line_item_detail A 
JOIN TMK B
ON A.Account_Key = B.Account_Key
AND A.Invoice_Time_Month_Key = B.Time_Month_Key
JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE
	((lower(partner_contract_type) like '%referral%' and lower(partner_contract_type) not like '%/%' and lower(commissions_role) like '%commissions%')
or (lower(partner_contract_type) like '%referral/reseller%' and lower(commissions_role) like '%commissions%'))
and lower(partner_contract_type) not like '%strategic%'
and lower(transaction_type) = 'inv'
AND Term>=12
AND upper(C.Partner_Comp_Type) = 'AWS'

--AND TOTAL>=0
GROUP BY
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END	
;
					
----------------------------------------
create or replace temp table AVG2 as 
SELECT
	Opportunity_Number,
	Term,
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
	AVG(TOTAL)								AS Three_Mo_Average,
	AVG(TOTAL_USD_GBP)							AS Three_Mo_Average_USD_GBP,
	Invoice_Type
FROM Opportunity2
WHERE
	TOTAL <> 0
GROUP BY
	Opportunity_Number,
Term,
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
Invoice_Type
;
----------------------------------------
create or replace temp table Comm2 as
SELECT
	Opportunity_Number,
	Term,
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
	CurrentTime_Month													AS Invoice_Time_Month_Key,
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	.5															AS Comm_Payout,
	.5*Three_Mo_Average_USD_GBP									AS Comm
FROM AVG2
WHERE  Three_Mo_Average_USD_GBP >=50;
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_referral
SELECT
	Opportunity_Number,
	Term,
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
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	cast(Comm_Payout as NUMERIC ) as Comm_Payout,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'AWS'

FROM Comm2;

----------------------------------------------------------------------------------------------
--Azure

create or replace temp table Opportunity3  as
SELECT 
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	SUM(TOTAL)								AS TOTAL,
	SUM(TOTAL_USD_GBP)							AS TOTAL_USD_GBP,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END					AS Invoice_Type
FROM 
	`rax-abo-72-dev`.sales.partner_program_line_item_detail A 
JOIN TMK B
ON A.Account_Key = B.Account_Key
AND A.Invoice_Time_Month_Key = B.Time_Month_Key
JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE
	((lower(Partner_Contract_Type) like '%Referral%' and lower(Partner_Contract_Type) not like '%/%' and lower(Commissions_Role) like '%commissions%')
OR (lower(Partner_Contract_Type) like '%Referral/Reseller%' and lower(Commissions_Role) like '%commissions%'))
AND lower(Partner_Contract_Type) not like '%Strategic%'
and lower(transaction_type) = 'inv'
AND Term>=12
AND lower(C.Partner_Comp_Type) = 'azure'

--AND TOTAL>=0
GROUP BY
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END	
;
					
----------------------------------------
create or replace temp table AVG3 as
SELECT
	Opportunity_Number,
	Term,
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
	AVG(TOTAL)								AS Three_Mo_Average,
	AVG(TOTAL_USD_GBP)							AS Three_Mo_Average_USD_GBP,
	Invoice_Type
FROM Opportunity3
WHERE
	TOTAL <> 0
GROUP BY
	Opportunity_Number,
Term,
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
Invoice_Type
;
----------------------------------------
create or replace temp table  Comm3 as
SELECT
	Opportunity_Number,
	Term,
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
	CurrentTime_Month													AS Invoice_Time_Month_Key,
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	.5															AS Comm_Payout,
	.5*Three_Mo_Average_USD_GBP									AS Comm
FROM AVG3
WHERE 	Three_Mo_Average_USD_GBP >=50
;
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_referral
SELECT
	Opportunity_Number,
	Term,
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
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	cast(Comm_Payout as NUMERIC ) as Comm_Payout,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'Azure'

FROM Comm3;
----------------------------------------------------------------------------------------------------------
--O365
create or replace temp table Opportunity4  as
SELECT 
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	SUM(TOTAL)								AS TOTAL,
	SUM(TOTAL_USD_GBP)							AS TOTAL_USD_GBP,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END					AS Invoice_Type
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
JOIN TMK B
ON A.Account_Key = B.Account_Key
AND A.Invoice_Time_Month_Key = B.Time_Month_Key
JOIN `rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE
	((lower(lower(Partner_Contract_Type)) like '%Referral%' and lower(lower(Partner_Contract_Type)) not like '%/%' and lower(lower(Commissions_Role)) like '%commissions%')
OR (lower(lower(Partner_Contract_Type)) like '%Referral/Reseller%' and lower(lower(Commissions_Role)) like '%commissions%'))
AND lower(lower(Partner_Contract_Type)) not like '%Strategic%'
and lower(transaction_type) = 'inv'
AND Term>=12
AND C.Partner_Comp_Type = 'O365'
AND Close_Date >= v_O365

--AND TOTAL>=0
GROUP BY
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END	
;
					
----------------------------------------
create or replace temp table AVG4 as
SELECT
	Opportunity_Number,
	Term,
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
	AVG(TOTAL)								AS Three_Mo_Average,
	AVG(TOTAL_USD_GBP)							AS Three_Mo_Average_USD_GBP,
	Invoice_Type
FROM Opportunity4
WHERE
	TOTAL <> 0
GROUP BY
	Opportunity_Number,
Term,
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
Invoice_Type
;
----------------------------------------
create or replace temp table Comm4 as
SELECT
	Opportunity_Number,
	Term,
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
	CurrentTime_Month													AS Invoice_Time_Month_Key,
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	.1															AS Comm_Payout,
	.1*Three_Mo_Average_USD_GBP									AS Comm
FROM AVG4
WHERE 
	Three_Mo_Average_USD_GBP >=50
	;

-------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_referral
SELECT
	Opportunity_Number,
	Term,
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
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	cast(Comm_Payout as NUMERIC ) as Comm_Payout,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'O365'

FROM Comm4;
-------------------------------------------------------------------------------------------------------------------
--Google
create or replace temp table Opportunity5 as
SELECT  
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	SUM(TOTAL)								AS TOTAL,
	SUM(TOTAL_USD_GBP)							AS TOTAL_USD_GBP,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END					AS Invoice_Type
FROM   `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
JOIN TMK B
ON A.Account_Key = B.Account_Key
AND A.Invoice_Time_Month_Key = B.Time_Month_Key
JOIN `rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE
	((lower(partner_contract_type) like '%referral%' and lower(partner_contract_type) not like '%/%' and lower(commissions_role) like '%commissions%')
or (lower(partner_contract_type) like '%referral/reseller%' and lower(commissions_role) like '%commissions%'))
and lower(partner_contract_type) not like '%strategic%'
and lower(transaction_type) = 'inv'
AND Term>=12
AND lower(C.Partner_Comp_Type) = 'google'
--AND Close_Date >= v_O365

--AND TOTAL>=0
GROUP BY
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END	
;
					
----------------------------------------
create or replace temp table  AVG5 as 
SELECT
	Opportunity_Number,
	Term,
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
	AVG(TOTAL)								AS Three_Mo_Average,
	AVG(TOTAL_USD_GBP)							AS Three_Mo_Average_USD_GBP,
	Invoice_Type
FROM Opportunity5
WHERE
	TOTAL <> 0
GROUP BY
	Opportunity_Number,
Term,
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
Invoice_Type;
----------------------------------------
create or replace temp table  Comm5 as
SELECT
	Opportunity_Number,
	Term,
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
	CurrentTime_Month													AS Invoice_Time_Month_Key,
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	.1															AS Comm_Payout,
	.1*Three_Mo_Average_USD_GBP									AS Comm


FROM AVG5
WHERE  Three_Mo_Average_USD_GBP >=50;
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_referral
SELECT
	Opportunity_Number,
	Term,
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
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	cast(Comm_Payout as NUMERIC ) as Comm_Payout,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'Google'

FROM Comm5;
----------------------------------------------------------------------------------------------------------
--VMWare
create or replace temp table Opportunity6 as
SELECT 
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	SUM(TOTAL)								AS TOTAL,
	SUM(TOTAL_USD_GBP)							AS TOTAL_USD_GBP,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END					AS Invoice_Type
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
JOIN TMK B
ON A.Account_Key = B.Account_Key
AND A.Invoice_Time_Month_Key = B.Time_Month_Key
JOIN `rax-abo-72-dev`.sales.dim_partner_nontraditional_products  C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE
	((lower(Partner_Contract_Type) like '%Referral%' and lower(Partner_Contract_Type) not like '%/%' and lower(Commissions_Role) like '%commissions%')
OR (lower(Partner_Contract_Type) like '%Referral/Reseller%' and lower(Commissions_Role) like '%commissions%'))
AND lower(Partner_Contract_Type) not like '%Strategic%'
and lower(transaction_type) = 'inv'
AND Term>=12
AND lower(C.Partner_Comp_Type) = 'vmware'
--AND Close_Date >= v_O365

--AND TOTAL>=0
GROUP BY
	Opportunity_Number,
	Term,
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
	INTL_Flag,
	CASE WHEN lower(Invoice_Source) = 'dedicated'
		 THEN 'Dedicated'
		 ELSE 'Cloud/Email' END	
;
					
----------------------------------------
create or replace temp table AVG6 as 
SELECT
	Opportunity_Number,
	Term,
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
	AVG(TOTAL)								AS Three_Mo_Average,
	AVG(TOTAL_USD_GBP)							AS Three_Mo_Average_USD_GBP,
	Invoice_Type
FROM Opportunity6
WHERE
	TOTAL <> 0
GROUP BY
	Opportunity_Number,
Term,
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
Invoice_Type;
----------------------------------------
create or replace temp table Comm6 as
SELECT
	Opportunity_Number,
	Term,
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
	CurrentTime_Month													AS Invoice_Time_Month_Key,
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	1														AS Comm_Payout,
	1*Three_Mo_Average_USD_GBP									AS Comm
FROM AVG6
WHERE  Three_Mo_Average_USD_GBP >=50;

----------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_referral
SELECT
	Opportunity_Number,
	Term,
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
	Three_Mo_Average,
	Three_Mo_Average_USD_GBP,
	cast(Comm_Payout as NUMERIC ) as Comm_Payout,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'VMWARE'

FROM Comm6
;
end;