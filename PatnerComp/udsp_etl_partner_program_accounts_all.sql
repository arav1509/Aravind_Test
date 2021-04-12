CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_program_accounts_all(V_Date date)
begin
---------------------------------------------------------------------------------------------------------
DECLARE CurrentMonthYear datetime;
DECLARE CurrentTime_Month int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;
-------------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=V_Date;
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);

-------------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_program_accounts_all where Time_Month_Key = CurrentTime_Month;
-------------------------------------------------------------------------------------------------------------------
create or replace temp table Partner_Accounts_All_Temp as
SELECT * --INTO #Partner_Accounts_All_Temp
FROM
	`rax-abo-72-dev`.sales.partner_accounts_all
WHERE Time_month_Key = CurrentTime_Month
AND Close_Date_Time_Month_Key <= CurrentTime_Month;

DELETE FROM Partner_Accounts_All_Temp
WHERE 
	Pay_Commissions = 0
--AND ifnull(Partner_Contract_Type,'Unknown') in ('','N/A','Unknown','US Legacy Unified Agreement','EMEA Legacy Unified Agreement','No Contract/Prospective Partner','US Legacy Unified Agreement')
AND Device_Number = '0'
AND lower(Source) = 'dedicated partner';

DELETE FROM Partner_Accounts_All_Temp
WHERE 
	--Pay_Commissions = 0
	lower(ifnull(Partner_Contract_Type,'Unknown')) in ('','n/a','unknown','us legacy unified agreement','emea legacy unified agreement','no contract/prospective partner','us legacy unified agreement')
AND Device_Number = '0'
AND lower(Source) = 'dedicated partner';


--DELETE A
--FROM #Partner_Accounts_All_Temp A
--WHERE 
--	Partner_Contract_Signed_Date is null

-------------------------------------------------------------------------------------------
create or replace temp table Partner_Program_Accounts_All_Temp  as 
SELECT DISTINCT 
	Opportunity_ID						as Opportunity_Number,
	A.Account_Num						as Account_Number,
	A.Device_Number,
	Device_End_Date,
	Term,
	Free_Days,
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
	Refresh_Date,
	Source as Program,
	'Dedicated' as Account_Source,
	Partner_Type,
	US_Partner_Type,
	Time_Month_Key,
	Category
FROM 
	Partner_Accounts_All_Temp A 
JOIN(
	SELECT
		Account_Num,
		Device_Number,
		max(Close_Date) as min_opp
	FROM Partner_Accounts_All_Temp 
	WHERE Time_Month_Key = CurrentTime_Month
	AND ( lower(SOURCE) = 'dedicated partner'
	OR	DDI = '99999999'
	OR (Account_Num is not null AND lower(Final_Opportunity_Type_Group) = 'mail'))
	AND Account_Num is not null
	GROUP BY Account_Num, Device_Number)B
ON A.Device_Number = B.Device_Number
AND A.Close_Date= B.min_opp
AND A.Account_Num = B.Account_Num
WHERE (lower(SOURCE) = 'dedicated partner'
OR	DDI = '99999999'
OR (A.Account_Num is not null AND lower(Final_Opportunity_Type_Group) = 'mail'))
AND A.Account_Num is not null
AND Device_End_Date >= `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(CurrentMonthYear)
AND Time_Month_Key = CurrentTime_Month
--------------------------------------------	
UNION All
--------------------------------------------
SELECT DISTINCT
	Opportunity_ID						as Opportunity_Number, 
	A.DDI									as Account_Number,
	Device_Number,
	Device_End_Date,
	Term,
	Free_Days,
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
	Refresh_Date,
	Source as Program,
	'Cloud'    as Account_Source,
	Partner_Type,
	US_Partner_Type,
	Time_Month_Key,
	Category
FROM 
	Partner_Accounts_All_Temp A 
JOIN(
	SELECT
		DDI,
		min(close_date) as min_close
	FROM Partner_Accounts_All_Temp 
	WHERE Time_Month_Key = CurrentTime_Month
	AND lower(SOURCE) <> 'dedicated partner'
	AND DDI is not null
	AND	DDI not in ('99999999','11111','111111','1111111')
	AND (( lower(Final_Opportunity_Type_Group) = 'cloud') or ( lower(Final_Opportunity_Type) = 'dedicated/private cloud' and DDI is not null)) --or Final_Opportunity_Type = 'Managed SysOps' or Final_Opportunity_Type = 'Managed Infrastructure') --Added SysOps and Infra 3/21/2016 per KL
	GROUP BY  DDI)B
ON A.DDI = B.DDI
AND A.Close_date = B.min_close
WHERE lower(SOURCE) <> 'dedicated partner'
AND A.DDI is not null
AND	A.DDI not in ('99999999','11111','111111','1111111')
AND ((Final_Opportunity_Type_Group = 'Cloud') or (lower(Final_Opportunity_Type) = 'dedicated/private cloud' and A.DDI is not null))--(Final_Opportunity_Type like '%Cloud%' or Final_Opportunity_Type = 'Managed SysOps' or Final_Opportunity_Type = 'Managed Infrastructure') --Added SysOps and Infra 3/21/2016 per KL
AND Device_End_Date >= `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(CurrentMonthYear)
AND Time_Month_Key = CurrentTime_Month
--------------------------------------------	
UNION All
--------------------------------------------
SELECT DISTINCT
	Opportunity_ID						as Opportunity_Number, 
	A.Email_Account_Num					as Account_Number,
	Device_Number,
	Device_End_Date,
	Term,
	Free_Days,
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
	Refresh_Date,
	Source as Program,
	'Email' as Account_Source,
	Partner_Type,
	US_Partner_Type,
	Time_Month_Key,
	Category
FROM 
	Partner_Accounts_All_Temp A 
JOIN(
	SELECT
		Email_Account_Num,
		min(close_date) as min_close
	FROM Partner_Accounts_All_Temp 
	WHERE Time_Month_Key = CurrentTime_Month
	AND Email_Account_Num is not null
	AND Email_Account_Num <> '99999999'
	AND Email_Account_Num <>' '
	AND lower(Final_Opportunity_Type) like '%mail%'
	GROUP BY  Email_Account_Num)B
ON A.Email_Account_Num = B.Email_Account_Num
AND A.Close_date = B.min_close
WHERE A.Email_Account_Num is not null
AND A.Email_Account_Num <> '99999999'
AND A.Email_Account_Num <>' '
AND lower(Final_Opportunity_Type) like '%mail%'
AND Device_End_Date >= `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(CurrentMonthYear)
AND Time_Month_Key = CurrentTime_Month;
-------------------------------------------------------------------------------------------	
INSERT INTO  `rax-abo-72-dev`.sales.partner_program_accounts_all
SELECT DISTINCT
	Opportunity_Number,
	Account_Number,
	Device_Number,
	Device_End_Date,
	Term,
	Free_Days,
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
	Refresh_Date,
	Program,
	Account_Source,
	CASE WHEN
		 (UPPER(PARTNER_CONTRACT_TYPE) LIKE ('AUS%') OR UPPER(PARTNER_CONTRACT_TYPE) LIKE ('EMEA%') OR UPPER(PARTNER_CONTRACT_TYPE) LIKE ('HK%') OR UPPER(PARTNER_CONTRACT_TYPE) LIKE ('%INTERNATIONAL%')) THEN 1
		 ELSE 0 END							AS INTL_Flag,
	Partner_Type,
	US_Partner_Type,
	Time_Month_Key,
	Category
FROM Partner_Program_Accounts_All_Temp
WHERE
	Pay_Commissions = 1
AND ( lower(Partner_Account_Type) in ('partner','vc','former partner','product alliance partner') or lower(program) = 'affiliate')
AND ifnull(UPPER(PARTNER_CONTRACT_TYPE),'UNKNOWN') nOT IN ('','N/A','UNKNOWN','US LEGACY UNIFIED AGREEMENT','EMEA LEGACY UNIFIED AGREEMENT','NO CONTRACT/PROSPECTIVE PARTNER','US LEGACY UNIFIED AGREEMENT')
AND Partner_Contract_Signed_Date is not null
;
--AND (Partner_Contract_Type not like ('AUS%') and Partner_Contract_Type not like ('EMEA%') and Partner_Contract_Type not like ('HK%'))
--------------------------------------------------------------------------------------------------------------
--CREATE INDEX IX_Account_Number ON dbo.Partner_Program_Accounts_All(Account_Number) 
--CREATE INDEX IX_Partner_Account ON dbo.Partner_Program_Accounts_All(Partner_Account) 
--CREATE INDEX IX_Program ON dbo.Partner_Program_Accounts_All(Program) 
--CREATE INDEX IX_Account_Source ON dbo.Partner_Program_Accounts_All(Account_Source) 
--------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_program_accounts_all
WHERE 
	INTL_FLAG = 1
AND lower(Partner_Account_Type) = 'former partner'
AND Time_Month_Key = CurrentTime_Month
;
--------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_program_accounts_all
WHERE
	Time_Month_Key = CurrentTime_Month
AND ((UPPER(PARTNER_CONTRACT_TYPE) LIKE '%REFERRAL%' AND UPPER(PARTNER_CONTRACT_TYPE) NOT LIKE '%/%' AND COMMISSIONS_ROLE LIKE '%COMMISSIONS%')
OR (UPPER(PARTNER_CONTRACT_TYPE) LIKE '%REFERRAL/RESELLER%' AND COMMISSIONS_ROLE LIKE '%COMMISSIONS%'))
AND UPPER(PARTNER_CONTRACT_TYPE) NOT LIKE '%STRATEGIC%'
AND upper(Category) NOT LIKE '%NEW%'
;

--------------------------------------------------------------------------------------------------------------
create or replace table `rax-abo-72-dev`.sales.partner_program_resellers as
SELECT 	* --INTO dbo.Partner_Program_Resellers
FROM
	`rax-abo-72-dev`.sales.partner_accounts_all A 
WHERE
	UPPER(PARTNER_CONTRACT_TYPE) IN ('AUS RESELLER AGREEMENT','US RESELLER AGREEMENT','EMEA RESELLER AGREEMENT','APAC RESELLER AGREEMENT','HK RESELLER AGREEMENT','LATAM RESELLER AGREEMENT')
and Pay_Commissions = 0;
--------------------------------------------------------------------------------------------------------------
create or replace temp table Exceptions as
SELECT --INTO #Exceptions
	Time_Month_Key,
	Account_Number,
	Device_Number,
	count(partner_account)	as Account_Count,
	'Dedicated'				as Source
FROM `rax-abo-72-dev`.sales.partner_program_accounts_all
WHERE
	lower(account_source) = 'dedicated' 
AND Device_Number <> '0'
AND Time_Month_Key = CurrentTime_Month
GROUP BY
	Time_Month_Key,
	Account_Number, 
Device_Number
HAVING 
	count(partner_account)>1
-------------------
UNION ALL
-------------------
SELECT
	Time_Month_Key,
	Account_Number,
	Device_Number,
	count(device_number)  as Account_Count,
	'Dedicated'					as Source

FROM `rax-abo-72-dev`.sales.partner_program_accounts_all
WHERE 
	lower(account_source) = 'dedicated' 
and Device_number = '0'
AND Time_Month_Key = CurrentTime_Month
GROUP BY
	Time_Month_Key,
	Account_Number,
Device_Number
HAVING
	count(device_number)>1
-------------------
UNION ALL
-------------------
SELECT
	Time_Month_Key,
	Account_Number,
	Device_Number,
	count(partner_account)  as Account_Count,
	'Cloud'					as Source
FROM `rax-abo-72-dev`.sales.partner_program_accounts_all
WHERE 
	lower(account_source) = 'cloud' 
GROUP BY
	Time_Month_Key,
	Account_Number,
Device_Number
HAVING
	count(partner_account)>1
-------------------
UNION ALL
-------------------
SELECT
	Time_Month_Key,
	Account_Number,
	Device_number,
	count(partner_account)	as Account_Count,
	'Email'					as Source
FROM `rax-abo-72-dev`.sales.partner_program_accounts_all
WHERE
	lower(account_source) = 'email'
GROUP BY
	Time_Month_Key,
	Account_Number,
Device_Number
HAVING
	count(partner_account)>1
	;

-------------------------------------------------------------------------------------------------------------------
create or replace table `rax-abo-72-dev`.sales.partner_program_exceptions as
SELECT 	A.*
FROM `rax-abo-72-dev`.sales.partner_program_accounts_all A
JOIN
	Exceptions B
ON A.Account_Number = B.Account_Number
AND A.Device_Number = B.Device_Number
AND A.Time_Month_Key = B.Time_Month_Key
;

-------------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_program_accounts_all
WHERE
	(Opportunity_Number in (Select Opportunity_Number from `rax-abo-72-dev`.sales.partner_program_exceptions)
AND Account_Number in (select Account_Number 		  from `rax-abo-72-dev`.sales.partner_program_exceptions)
AND Device_Number in (select Device_Number 			  from `rax-abo-72-dev`.sales.partner_program_exceptions))
AND Time_Month_Key = CurrentTime_Month
;
-------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_compensation_custom_fixed_percentages
SELECT DISTINCT
	Opportunity_Number,
	A.Partner_Account_RV_EXT_ID,
	A.Partner_Account_Name,
	0.00,
	B.Account_Name
FROM
	`rax-abo-72-dev`.sales.partner_program_accounts_all A 
JOIN `rax-abo-72-dev`.sales.partner_accounts_all B 
ON A.Opportunity_Number = B.Opportunity_ID 
AND A.Time_Month_Key = B.Time_Month_Key
WHERE
	lower(A.Tier_Level) = 'Custom'
AND 
	Opportunity_Number not in (select distinct ifnull(Opportunity_Number,'0') from `rax-abo-72-dev`.sales.partner_compensation_custom_fixed_percentages)
AND A.Partner_Account_RV_EXT_ID <> '1169041';
-------------------------------------------------------------------------------------------------------------------
end;