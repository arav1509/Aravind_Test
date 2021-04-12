CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_all_v2(V_Date date)
begin

DECLARE CurrentMonthYear datetime;
DECLARE CurrentTime_Month  int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;
-----------------------------------------------------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=V_Date;
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);
-----------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
--if exists (select * from dbo.sysobjects where id = object_id(N'Partner_Program_Accounts_All3') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
--drop table dbo.Partner_Program_Accounts_All3
-------------------------------------------------------------------------------------------------------------------
create or replace temp table Partner_Accounts_All_Temp as
SELECT *
FROM
	`rax-abo-72-dev`.sales.partner_accounts_all 
WHERE Time_month_Key = CurrentTime_Month;

---------------------------------------------------------------------------------------------------------------------
create or replace temp table	Partner_Program_Accounts_All_Temp  as
SELECT DISTINCT 
	Opportunity_ID						as Opportunity_Number,
	Master_Opportunity_ID,
	A.Account_Num						as Account_Number,
	Account_Name,
	A.Device_Number,
	Device_End_Date,
	Term,
	Free_Days,
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
	PartnerAssociated,
	Partner_Divested

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
	OR (Account_Num is not null AND lower(Final_Opportunity_Type) = 'mail'))
	AND Account_Num is not null
	GROUP BY Account_Num, Device_Number)B
ON A.Device_Number = B.Device_Number
AND A.Close_Date= B.min_opp
AND A.Account_Num = B.Account_Num
WHERE ( lower(SOURCE) = 'dedicated partner'
OR	DDI = '99999999'
OR (A.Account_Num is not null AND lower(Final_Opportunity_Type) = 'mail'))
AND A.Account_Num is not null
AND Time_Month_Key = CurrentTime_Month
--------------------------------------------	
UNION All
--------------------------------------------
SELECT DISTINCT
	Opportunity_ID						as Opportunity_Number, 
	Master_Opportunity_ID,
	A.DDI									as Account_Number,
	Account_Name,
	Device_Number,
	Device_End_Date,
	Term,
	Free_Days,
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
	PartnerAssociated,
	Partner_Divested
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
	AND ( lower(Final_Opportunity_Type_Group) = 'cloud') --or final_opportunity_type = 'managed sysops' or final_opportunity_type = 'managed infrastructure') --added sysops and infra 3/21/2016 per kl
	GROUP BY  DDI)B
ON A.DDI = B.DDI
AND A.Close_date = B.min_close
WHERE lower(SOURCE) <> 'dedicated partner'
AND A.DDI is not null
AND	A.DDI not in ('99999999','11111','111111','1111111')
AND lower(Final_Opportunity_Type_Group) = 'cloud'--(Final_Opportunity_Type like '%Cloud%' or Final_Opportunity_Type = 'Managed SysOps' or Final_Opportunity_Type = 'Managed Infrastructure') --Added SysOps and Infra 3/21/2016 per KL
AND Time_Month_Key = CurrentTime_Month
--------------------------------------------	
UNION All
--------------------------------------------
SELECT DISTINCT
	Opportunity_ID						as Opportunity_Number, 
	Master_Opportunity_ID,
	A.Email_Account_Num					as Account_Number,
	Account_Name,
	Device_Number,
	Device_End_Date,
	Term,
	Free_Days,
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
	PartnerAssociated,
	Partner_Divested
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
AND Time_Month_Key = CurrentTime_Month
;
	
-------------------------------------------------------------------------------------------	
create or replace table `rax-abo-72-dev`.sales.partner_program_accounts_all3 as
SELECT DISTINCT
	Opportunity_Number,
	Master_Opportunity_ID,
	Account_Number,
	Account_Name,
	Device_Number,
	Device_End_Date,
	Term,
	Free_Days,
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
		 ( upper(Partner_Contract_Type) like ('AUS%') or upper(Partner_Contract_Type) like ('EMEA%') or upper(Partner_Contract_Type) like ('HK%')) THEN 1
		 ELSE 0 END							AS INTL_Flag,
	Partner_Type,
	US_Partner_Type,
	PartnerAssociated,
	Partner_Divested
FROM
	Partner_Program_Accounts_All_Temp	
WHERE
	--Pay_Commissions = 1
( lower(Partner_Account_Type) in ('partner','vc','former partner','prospective partner','product alliance partner') or lower(program) = 'affiliate' or partner_account_rv_ext_id = '186370' or partner_account_rv_ext_id = '205830')--and isnull(partner_contract_type,'unknown') not in ('','n/a','unknown','us legacy unified agreement','emea legacy unified agreement','no contract/prospective partner','us legacy unified agreement')
;
end;