CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_compensation_strategic(V_Date date)
begin

-------------------------------------------------------------------------------------------------------------------
DECLARE CurrentMonthYear datetime;
DECLARE v_O365 datetime;
DECLARE CurrentTime_Month int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;
-------------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=V_Date;
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);
SET v_O365 = '2016-07-19';
-------------------------------------------------------------------------------------------------------------------
DELETE FROM  `rax-abo-72-dev`.sales.partner_compensation_strategic WHERE Invoice_Time_Month_Key=CurrentTime_Month;
-------------------------------------------------------------------------------------------------------------------
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
	INTL_Flag,
	Invoice_Time_Month_Key,
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP 
FROM 
	 `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
LEFT JOIN
	 `rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
--AND C.Partner_Comp = 1
WHERE 
	(((lower(partner_contract_type) like '%strategic%'
or lower(partner_contract_type) = 'custom agreement'
or lower(partner_contract_type) = 'us master agent agreement')
and term>=12) or lower(partner_contract_type) = 'us strategic agency agreement')
and lower(transaction_type) = 'inv'
AND Invoice_Time_Month_Key =CurrentTime_Month
AND C.Product_Group is null
AND lower(A.Product_Group) not like '%colocation%'
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
	INTL_Flag,
	Invoice_Time_Month_Key
union all
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
(((lower(partner_contract_type) like '%strategic%'
or lower(partner_contract_type) = 'custom agreement'
or lower(partner_contract_type) = 'us master agent agreement')
and term>=12) or lower(partner_contract_type) = 'us strategic agency agreement')
and lower(transaction_type) = 'inv'
AND Invoice_Time_Month_Key =CurrentTime_Month
AND C.Partner_Comp_Type = 'O365'
AND Close_Date < v_O365

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
	INTL_Flag,
	Invoice_Time_Month_Key;
----------------------------------------

create or replace temp table Comm_Temp as
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
	Invoice_Time_Month_Key,
	Total,
	Total_USD_GBP,
	Comm											AS Comm_Percentage,
	TOTAL_USD_GBP*ifnull(Comm,.10)						AS Comm
FROM Opportunity A
LEFT JOIN `rax-abo-72-dev`.sales.partner_compensation_commission_tiers B
ON A.Tier_Level = B.Tier
AND lower(Type) = 'strategic'
;
-------------------------------
UPDATE Comm_Temp com
SET
	com.Comm_Percentage = Fix_Percentage,
	com.Comm = A.Total_USD_GBP*Fix_Percentage
FROM
	Comm_Temp A
LEFT JOIN
	`rax-abo-72-dev`.sales.partner_compensation_custom_fixed_percentages B
ON A.Opportunity_Number = B.Opportunity_Number
WHERE lower(A.Tier_Level) = 'Custom';
-------------------------------------------------------------------------------------------------------------------
UPDATE Comm_Temp com
SET
	com.Comm_Percentage = CASE WHEN a.Term = 6 THEN 0.5
						   WHEN a.Term > 6 AND a.Term <=12 THEN 1
						   WHEN a.Term > 12 AND a.Term <=18 THEN 1.3
						   WHEN a.Term > 18 AND a.Term <= 24 THEN 1.5
						   WHEN a.Term > 24 AND a.Term <= 30 THEN 1.8
						   WHEN a.Term > 30 AND a.Term <= 36 THEN 2
						   ELSE 0 END,
	com.Comm =  CASE WHEN a.Term = 6 THEN 0.5*A.TOTAL_USD_GBP
						   WHEN a.Term > 6 AND a.Term <=12 THEN 1*A.TOTAL_USD_GBP
						   WHEN a.Term > 12 AND a.Term <=18 THEN 1.3*A.TOTAL_USD_GBP
						   WHEN a.Term > 18 AND a.Term <= 24 THEN 1.5*A.TOTAL_USD_GBP
						   WHEN a.Term > 24 AND a.Term <= 30 THEN 1.8*A.TOTAL_USD_GBP
						   WHEN a.Term > 30 AND a.Term <= 36 THEN 2*A.TOTAL_USD_GBP
						   ELSE 0 END
FROM
	Comm_Temp a
WHERE 
	a.Partner_Account_RV_EXT_ID = '1169041';
-------------------------------------------------------------------------------------------------------------------
UPDATE Comm_Temp com
SET
	com.Comm_Percentage = 0.13,
	com.Comm = 0.13*A.TOTAL_USD_GBP
FROM
	Comm_Temp A
WHERE
	lower(A.Tier_Level) = 'strategic tier level 3';
-------------------------------------------------------------------------------------------------------------------
UPDATE Comm_Temp com
SET
	com.Comm_Percentage = 0.15,
	com.Comm = 0.15*A.TOTAL_USD_GBP
FROM
	Comm_Temp A
WHERE
	lower(A.Tier_Level) = 'strategic tier level 2';
-------------------------------------------------------------------------------------------------------------------
UPDATE Comm_Temp com
SET
	com.Comm_Percentage = 0.18,
	com.Comm = 0.18*A.TOTAL_USD_GBP
FROM
	Comm_Temp A
WHERE
	lower(A.Tier_Level) = 'strategic tier level 1';
-------------------------------------------------------------------------------------------------------------------
UPDATE Comm_Temp com
SET
	com.Comm_Percentage = 0.11,
	com.Comm = 0.11*A.TOTAL_USD_GBP
FROM
	Comm_Temp A
WHERE
	lower(A.Tier_Level) = 'strategic tier level 3 – tier down';
-------------------------------------------------------------------------------------------------------------------
UPDATE Comm_Temp com
SET
	com.Comm_Percentage = 0.13,
	com.Comm = 0.13*A.TOTAL_USD_GBP
FROM
	Comm_Temp A
WHERE
	lower(A.Tier_Level) = 'Strategic Tier Level 2 – Tier Down';
-------------------------------------------------------------------------------------------------------------------
UPDATE Comm_Temp com
SET
	com.Comm_Percentage = 0.16,
	com.Comm = 0.16*A.TOTAL_USD_GBP
FROM
	Comm_Temp A
WHERE
	Lower(A.Tier_Level) = 'strategic tier level 1 – tier down';
-------------------------------------------------------------------------------------------------------------------

INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_strategic
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
	SUM(Total) as Total,
	SUM(Total_USD_GBP) as Total_USD_GBP,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'Traditional'

FROM Comm_Temp
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
	Invoice_Time_Month_Key,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End,
	INTL_Flag
	;
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
	INTL_Flag,
	Invoice_Time_Month_Key,
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE 
	(((lower(Partner_Contract_Type) like '%strategic%'
or lower(Partner_Contract_Type) = 'custom agreement'
or lower(Partner_Contract_Type) = 'us master agent agreement')
and term>=12) or lower(Partner_Contract_Type) = 'us strategic agency agreement')
and lower(transaction_type) = 'inv'
and invoice_time_month_key =currenttime_month
and lower(c.partner_comp_type) = 'aws'

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
	INTL_Flag,
	Invoice_Time_Month_Key
	;
----------------------------------------
create or replace temp table Comm_Temp2 as 
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
	Invoice_Time_Month_Key,
	Total,
	Total_USD_GBP,
	.10											AS Comm_Percentage,
	TOTAL_USD_GBP*.10						AS Comm
FROM Opportunity2 A;
-------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_strategic
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
	SUM(Total) as Total,
	SUM(Total_USD_GBP) as Total_USD_GBP,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'AWS'

FROM Comm_Temp2
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
	Invoice_Time_Month_Key,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End,
	INTL_Flag;
-------------------------------------------------------------------------------

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
	INTL_Flag,
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
(((lower(Partner_Contract_Type) like '%strategic%'
or lower(Partner_Contract_Type) = 'custom agreement'
or lower(Partner_Contract_Type) = 'us master agent agreement')
and term>=12) or lower(Partner_Contract_Type) = 'us strategic agency agreement')
and lower(transaction_type) = 'inv'
and invoice_time_month_key =currenttime_month
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
	INTL_Flag,
	Invoice_Time_Month_Key;
----------------------------------------
create or replace temp table Comm_Temp3 as 
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
	Invoice_Time_Month_Key,
	Total,
	Total_USD_GBP,
	CASE WHEN Partner_Account_RV_EXT_ID = '1167317' THEN .20 ELSE .10 END	AS Comm_Percentage,
	CASE WHEN Partner_Account_RV_EXT_ID = '1167317' THEN TOTAL_USD_GBP*.20 ELSE TOTAL_USD_GBP*.10 END AS Comm
FROM Opportunity3 A;
-------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_strategic
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
	SUM(Total) as Total,
	SUM(Total_USD_GBP) as Total_USD_GBP,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'Azure'

FROM Comm_Temp3
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
	Invoice_Time_Month_Key,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End,
	INTL_Flag
	;
--------------------------------------------------------------------------------
--O365 --Decomm Upon approval and Activate #Opportunity10 logic
create or replace temp table ONEOFF as 
SELECT
Opportunity_Number 
FROM `rax-abo-72-dev`.sales.partner_program_accounts_all 
WHERE Time_Month_Key = CurrentTime_Month
AND Account_Number = '5671562';
--------------------------------------------------------------------------------
create or replace temp table  Opportunity4 as  
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
	(((lower(Partner_Contract_Type) like '%strategic%'
or lower(Partner_Contract_Type) = 'custom agreement'
or lower(Partner_Contract_Type) = 'us master agent agreement')
and term>=12) or lower(Partner_Contract_Type) = 'us strategic agency agreement')
and lower(transaction_type) = 'inv'
and invoice_time_month_key =currenttime_month
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
	INTL_Flag,
	Invoice_Time_Month_Key;
----------------------------------------
----------------------------------------
create or replace temp table Comm_Temp4 as 
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
	Invoice_Time_Month_Key,
	Total,
	Total_USD_GBP,
	CASE WHEN Opportunity_Number in (select distinct Opportunity_Number from ONEOFF) THEN .05 ELSE .10	END	AS Comm_Percentage,
	CASE WHEN Opportunity_Number in (select distinct Opportunity_Number from ONEOFF) THEN TOTAL_USD_GBP*.05 ELSE TOTAL_USD_GBP*.10	END	AS Comm
FROM Opportunity4 A;
-------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_strategic
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
	SUM(Total) as Total,
	SUM(Total_USD_GBP) as Total_USD_GBP,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'O365'

FROM
	Comm_Temp4
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
	Invoice_Time_Month_Key,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End,
	INTL_Flag;
--------------------------------------------------------------------------------
--Google
create or replace temp table Opportunity5  as 
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
	Invoice_Time_Month_Key,
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail A 
JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE 
	(((lower(Partner_Contract_Type) like '%strategic%'
or lower(Partner_Contract_Type) = 'custom agreement'
or lower(Partner_Contract_Type) = 'us master agent agreement')
and term>=12) or lower(Partner_Contract_Type) = 'us strategic agency agreement')
and lower(transaction_type) = 'inv'
and invoice_time_month_key =currenttime_month
and lower(c.partner_comp_type) = 'google'
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
	INTL_Flag,
	Invoice_Time_Month_Key;
----------------------------------------
create or replace temp table  Comm_Temp5 as
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
	Invoice_Time_Month_Key,
	Total,
	Total_USD_GBP,
	.10											AS Comm_Percentage,
	TOTAL_USD_GBP*.10						AS Comm
FROM Opportunity5 A;
-------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_strategic
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
	SUM(Total) as Total,
	SUM(Total_USD_GBP) as Total_USD_GBP,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'Google'

FROM
	Comm_Temp5
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
	Invoice_Time_Month_Key,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End,
	INTL_Flag;
--------------------------------------------------------------------------------
--VMWare
create or replace temp table Opportunity6  as 
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
	Invoice_Time_Month_Key,
	SUM(TOTAL)					AS TOTAL,
	SUM(TOTAL_USD_GBP)				AS TOTAL_USD_GBP
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail  A 
JOIN
	`rax-abo-72-dev`.sales.dim_partner_nontraditional_products C 
ON A.Product_Group = C.Product_Group
AND A.Product_Type = C.Product_Type
AND C.Partner_Comp = 1
WHERE 
	(((lower(Partner_Contract_Type) like '%strategic%'
or lower(Partner_Contract_Type) = 'custom agreement'
or lower(Partner_Contract_Type) = 'us master agent agreement')
and term>=12) or lower(Partner_Contract_Type) = 'us strategic agency agreement')
and lower(transaction_type) = 'inv'
and invoice_time_month_key =currenttime_month
and lower(c.partner_comp_type)= 'vmware'
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
	INTL_Flag,
	Invoice_Time_Month_Key;
----------------------------------------
create or replace temp table  Comm_Temp6 as
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
	Invoice_Time_Month_Key,
	Total,
	Total_USD_GBP,
	.075										AS Comm_Percentage,
	TOTAL_USD_GBP*.075					AS Comm
FROM Opportunity6 A;
-------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_strategic
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
	SUM(Total) as Total,
	SUM(Total_USD_GBP) as Total_USD_GBP,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'VMWare'

FROM
	Comm_Temp6
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
	Invoice_Time_Month_Key,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End,
	INTL_Flag;

create or replace temp table Opportunity8 as	
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
	(( upper(Tier_Level) like '%SPA%')
AND Term>=12) 
AND upper(Transaction_Type) = 'INV'
AND Invoice_Time_Month_Key =CurrentTime_Month
AND upper(C.Partner_Comp_Type) = 'COLO'
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
	INTL_Flag,
	Invoice_Time_Month_Key;
----------------------------------------
create or replace temp table  Comm_Temp8 as 
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
	Invoice_Time_Month_Key,
	Total,
	Total_USD_GBP,
	.10										AS Comm_Percentage,
	TOTAL_USD_GBP*.10					AS Comm
FROM Opportunity8 A;
-------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_strategic
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
	SUM(Total) as Total,
	SUM(Total_USD_GBP) as Total_USD_GBP,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End																as Comm_Type,
	INTL_Flag,
	'Colo'

FROM
	Comm_Temp8
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
	Invoice_Time_Month_Key,
	Comm_Percentage,
	Comm,
	Case When lower(Commissions_Role) like '%credit%' then 'Credit'
		 When lower(Commissions_Role) like '%comm%' then 'Commissions'
		 End,
	INTL_Flag
;

end;