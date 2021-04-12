CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_cloud_smb_ib_email_apps_all(v_date date)
begin
DECLARE CurrentMonthYear  datetime;
DECLARE CurrentSH_Date int64;
DECLARE CurrentTime_Month int64;
DECLARE FirstdayofNextMonth  datetime;
DECLARE MaxSH_Date int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;
-------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=cast(CURRENT_DATE() as datetime);
SET FirstdayofNextMonth=`rax-abo-72-dev`.bq_functions.udf_firstdayofnextmonth(CurrentMonthYear);
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);
SET MaxSH_Date =(SELECT MAX(Time_Month_Key) FROM `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy);
SET CurrentSH_Date=(ifnull((	SELECT MAX(Time_Month_Key) 
								FROM `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy 
								WHERE Time_Month_Key= `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear))
							,MaxSH_Date)
					);

--SA3
create or replace temp table SA3Invoices as
SELECT DISTINCT
	A.ACT_AccountID,
	First_Invoice_Month,
	First_Invoiced_Date,
	First_Invoice_Amount
--INTO	#SA3Invoices							
FROM
(
SELECT
	A.ACT_AccountID,
	First_Invoice_Month				AS First_Invoice_Month,
	First_Invoiced_Date				AS First_Invoiced_Date,
	TotalPrice						AS First_Invoice_Amount
FROM
(
SELECT DISTINCT
	ACT_AccountID,
	MIN(Invoiced_Date_Time_Month)	AS First_Invoice_Month
FROM
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info A  
WHERE
	upper(Invoice_Source) IN ('SA3')
GROUP BY
	ACT_AccountID
)A
INNER JOIN
(
SELECT DISTINCT
	ACT_AccountID,
	Invoiced_Date_Time_Month	AS Invoiced_Date_Time_Month,
	First_Invoiced_Date			AS First_Invoiced_Date,
	SUM(TotalPrice)				AS TotalPrice,
	SUM(TotalPrice_Normalized)	AS TotalPrice_Normalized
FROM `rax-abo-72-dev`.sales.salesforce_cloud_billing_info A  
WHERE
	upper(Invoice_Source) IN ('SA3')
GROUP BY
	ACT_AccountID,
	First_Invoiced_Date,
	Invoiced_Date_Time_Month
)B	
ON A.ACT_AccountID=B.ACT_AccountID
AND A.First_Invoice_Month=B.Invoiced_Date_Time_Month
)a;
---------------------------------------------------------------------------------------------------------------
create or replace temp table OracleInvoices as
--Oracle
SELECT DISTINCT
	A.ACT_AccountID,
	First_Invoice_Month,
	First_Invoiced_Date,
	First_Invoice_Amount
--INTO	#OracleInvoices							
FROM
(
SELECT
	A.ACT_AccountID,
	First_Invoice_Month				AS First_Invoice_Month,
	First_Invoiced_Date				AS First_Invoiced_Date,
	TotalPrice						AS First_Invoice_Amount
FROM
(
SELECT DISTINCT
	ACT_AccountID,
	MIN(Invoiced_Date_Time_Month)	AS First_Invoice_Month
FROM
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info A  
WHERE
	upper(Invoice_Source) IN ('ORACLE')
GROUP BY
	ACT_AccountID
)A
INNER JOIN
(
SELECT DISTINCT
	ACT_AccountID,
	Invoiced_Date_Time_Month	AS Invoiced_Date_Time_Month,
	First_Invoiced_Date			AS First_Invoiced_Date,
	SUM(TotalPrice)				AS TotalPrice,
	SUM(TotalPrice_Normalized)	AS TotalPrice_Normalized
FROM
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info A  
WHERE
	upper(Invoice_Source) IN ('ORACLE')
GROUP BY
	ACT_AccountID,
	First_Invoiced_Date,
	Invoiced_Date_Time_Month
)B	
ON A.ACT_AccountID=B.ACT_AccountID
AND A.First_Invoice_Month=B.Invoiced_Date_Time_Month
)a;
---------------------------------------------------------------------------------------------------------------
create or replace temp table Cloud_SMB_IB_Email_Apps_ALL as
--IB
SELECT DISTINCT
	A.DDI										AS Cloud_Account,
	Email_Account_Num							AS Email_Account_Num, 
	Account_Name								AS Cloud_Account_Name,
	B.Email_Account_Type,
	B.Email_Core_Account_Num,
	CLOUD_Desired_Billing_Date,
	CLOUD_Account_Status,
	Cloud_Account_Create_Date,
	OPPORTUNITY_ID,
	Close_Date,
	ACCOUNTID,
	Account_Owner,
	Account_Owner_ID,
	Account_Owner_Is_Active,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Num,
	Account_Type,
	Account_Sub_Type,
	Opportunity_Type,
	Billing_Name								AS Opportunity_Name,
	Opportunity_Owner,
	Opportunity_Owner_ID,
	Opportunity_Owner_Is_Active,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opp_ISDELETED,
	'No Split'									AS Split_Category,
	100											AS Split_Percentage,
	Cloud_Username,
	Category,
	Final_Opportunity_Type						AS Final_Opportunity_Type,
	Opportunity_Sub_Type,
	'0125000000052evAAA'						AS  Record_Type,
	CASE	
		WHEN 
			lower(Cloud_Account_Source)='cloud uk'
		THEN
			'GBP'
		ELSE
			'USD'
	END											AS CURRENCYISOCODE,
	ifnull(APPROVAL_AMOUNT_Converted,0)			AS APPROVAL_AMOUNT_Converted,
	'Closed Won'								AS StageName,
	ON_DEMAND_RECONCILED,
	Solution_Engineer,
	Solution_Engineer_ID,
	Solution_Engineer_Is_Active					AS Solution_Engineer_Active,
	Additional_Solution_Engineer,
	Additional_Solution_Engineer_ID,
	Additional_Solution_Engineer_Is_Active		AS Additional_Solution_Engineer_Active,
	Additional_Sales_Rep,
	Additional_Sales_Rep_ID,
	Additional_Sales_Rep_Is_Active,	
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	Max_Lead_Generator_ID,
	Lead_Date_Passed,
	Reseller_Partner_Account,
	Partner_Account,
	Partner_Role,
	Channel,
	CAST('' AS string)						AS Commission_Referral_Type,
	Mail_1st_Month_Invoice,
	--Mail_Reseller_1st_Month_Invoice,
	Mail_Activation,
	Cloud_Account_Source,
	CAST('1900-01-01'	 as datetime)				AS As_of_Date,
	Territory, 
	LPID, 
	PLATFORM, 
	PLATFORM_SUB_CATEGORY, 
	BUCKET_INFLUENCE, 
	BUCKET_SOURCE, 
	LEADSOURCE, 
	FOCUS_AREA, 
	Partner_Role_Role, 
	Partner_Role_Name, 
	Partner_Role_StatusX,
	MARKETING_SOURCED
--INTO	#Cloud_SMB_IB_Email_Apps_ALL
FROM
(
SELECT DISTINCT
	DDI,
	MIN(Close_Date)  AS First_Closed_Date
FROM  
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot A
INNER JOIN
	`rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy B
ON A.Account_Owner_Role=B.Reporting_Team
WHERE
	lower(Final_Opportunity_Type) ='mail'
AND	Opp_ISDELETED ='N'
AND Time_Month_Key=CurrentSH_Date
AND B.Is_Active=1
--AND (Business_Unit Like '%Acquisition%' OR Business_Unit like '%Mail%' OR Business_Unit Like '%LATAM%')
AND (lower(business_unit) like '%commercial%' or lower(business_unit) like '%mail%' or lower(business_unit) like '%latam%' or lower(business_unit) like '%marketing%' or lower(business_unit) like '%cloud office%')
AND lower(ACQ_OR_IB) in ('ib','acq_ib_both')
AND Close_Date	< FirstdayofNextMonth	
AND ifnull( lower(Cloud_Account_Source),'cloud unknown')<>'cloud unknown'
GROUP BY
	DDI
)A
INNER JOIN
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot B
ON A.First_Closed_Date=B.Close_Date
AND A.DDI=B.DDI
INNER JOIN
	`rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy C
ON B.Account_Owner_Role=C.Reporting_Team
WHERE
	lower(Final_Opportunity_Type) ='mail'
AND	Opp_ISDELETED ='N'
AND Time_Month_Key=CurrentSH_Date
AND lower(STAGENAME) = ('closed won')
AND C.Is_Active=1
--AND (Business_Unit Like '%Acquisition%' OR Business_Unit like '%Mail%' OR Business_Unit Like '%LATAM%')
AND (lower(business_unit) like '%commercial%' or lower(business_unit) like '%mail%' or lower(business_unit) like '%latam%' or lower(business_unit) like '%marketing%' or lower(business_unit) like '%cloud office%')
AND lower(ACQ_OR_IB) in ('ib','acq_ib_both')
AND Close_Date	< FirstdayofNextMonth	
AND ifnull(lower(Cloud_Account_Source),'cloud unknown')<>'cloud unknown';
---------------------------------------------------------------------------------------------------------------
create or replace temp table IBDupes as
SELECT 
	* 
--INTO	#IBDupes
FROM 
	Cloud_SMB_IB_Email_Apps_ALL where Cloud_Account 
IN 
(
SELECT DISTINCT
	Cloud_Account
FROM
(
SELECT
	Cloud_Account,
	COUNT(Cloud_Account) AS Count
FROM
	Cloud_SMB_IB_Email_Apps_ALL
GROUP BY
	Cloud_Account
HAVING
	COUNT(Cloud_Account) >1
)A
);
---------------------------------------------------------------------------------------------------------------
DELETE FROM Cloud_SMB_IB_Email_Apps_ALL WHERE Cloud_Account in (SELECT Cloud_Account from IBDupes);
-------------------------------------------------------------------------------------------------------------
Insert into
	Cloud_SMB_IB_Email_Apps_ALL
SELECT
	A.Cloud_Account,
	Email_Account_Num, 
	Cloud_Account_Name,
	B.Email_Account_Type,
	B.Email_Core_Account_Num,
	CLOUD_Desired_Billing_Date,
	CLOUD_Account_Status,
	Cloud_Account_Create_Date,
	OPPORTUNITY_ID,
	Close_Date,
	ACCOUNTID,
	Account_Owner,
	Account_Owner_ID,
	Account_Owner_Is_Active,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Num,
	Account_Type,
	Account_Sub_Type,
	Opportunity_Type,
	Opportunity_Name,
	Opportunity_Owner,
	Opportunity_Owner_ID,
	Opportunity_Owner_Is_Active,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opp_ISDELETED,
	Split_Category,
	Split_Percentage,
	Cloud_Username,
	Category,
	Final_Opportunity_Type,
	Opportunity_Sub_Type,
	Record_Type,
	CURRENCYISOCODE,
	APPROVAL_AMOUNT_Converted,
	StageName,
	ON_DEMAND_RECONCILED,
	Solution_Engineer,
	Solution_Engineer_ID,
	Solution_Engineer_Active,
	Additional_Solution_Engineer,
	Additional_Solution_Engineer_ID,
	Additional_Solution_Engineer_Active,
	Additional_Sales_Rep,
	Additional_Sales_Rep_ID,
	Additional_Sales_Rep_Is_Active,	
	Is_Linked_Account,
	Is_Consolidated_Billing,
	Is_Internal_Account,
	Max_Lead_Generator_ID,
	Lead_Date_Passed,
	Reseller_Partner_Account,
	Partner_Account,
	Partner_Role,
	Channel,
	Commission_Referral_Type,
	Mail_1st_Month_Invoice,
	--Mail_Reseller_1st_Month_Invoice,
	Mail_Activation,
	Cloud_Account_Source,
	current_datetime()					AS As_of_Date,
	Territory, 
	LPID, 
	PLATFORM, 
	PLATFORM_SUB_CATEGORY, 
	BUCKET_INFLUENCE, 
	BUCKET_SOURCE, 
	LEADSOURCE, 
	FOCUS_AREA, 
	Partner_Role_Role, 
	Partner_Role_Name, 
	Partner_Role_StatusX,
	MARKETING_SOURCED
FROM
(
SELECT 
	Cloud_Account,
	MIN(OPPORTUNITY_ID) Min_Opp
FROM
	IBDupes
GROUP BY
	Cloud_Account
)A
INNER JOIN
	IBDupes B
ON A.Cloud_Account=B.Cloud_Account
AND A.Min_Opp=B.OPPORTUNITY_ID;
---------------------------------------------------------------------------------------------------------------
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Opportunity_Type= ' '
WHERE
	Opportunity_Type IS  NULL OR Opportunity_Type='0';
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Opportunity_Owner_ID= '0'
WHERE
	Opportunity_Owner_ID=' ' OR Opportunity_Owner_ID='0';
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Opportunity_Owner_Is_Active= 'true'
WHERE
	Opportunity_Owner_ID='0';
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Opp_ISDELETED= 'N'
WHERE
	lower(Opp_ISDELETED)='unknown' ;
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Solution_Engineer= ' '
WHERE
	Solution_Engineer IS  NULL OR Solution_Engineer='0';
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Solution_Engineer= ' '
WHERE
	lower(Solution_Engineer)='unknown';
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Solution_Engineer_ID= ' '
WHERE
	Solution_Engineer=' ';
-----------------------------------------------------		
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Solution_Engineer_Active= ' '
WHERE
	Solution_Engineer=' '	;
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Additional_Solution_Engineer= ' '
WHERE
	Additional_Solution_Engineer IS  NULL OR Additional_Solution_Engineer='0';
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Additional_Solution_Engineer= ' '
WHERE
	Additional_Solution_Engineer='Unknown'	;
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Additional_Solution_Engineer_ID= ' '
WHERE
	Additional_Solution_Engineer=' '	;
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Additional_Solution_Engineer_Active= ' '
WHERE
	Additional_Solution_Engineer=' ';
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Max_Lead_Generator_ID= ' '
WHERE
	Max_Lead_Generator_ID IS  NULL OR Max_Lead_Generator_ID='0';
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Reseller_Partner_Account= ' '
WHERE
	Reseller_Partner_Account IS  NULL OR Reseller_Partner_Account='0';
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Partner_Role= ' ',
	Channel=' ',
	Commission_Referral_Type=' '	
WHERE
	Reseller_Partner_Account=' ';
-----------------------------------------------------	
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET 
	Partner_Role= '(Cloud Reseller only)–Credit Acct/DDI',
	Channel='Referral',
	Commission_Referral_Type='Partner Referral'	
WHERE
	Reseller_Partner_Account<>' '	;

UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET
	Opportunity_Owner_Role_Segment='Unknown'
WHERE
	(Opportunity_Owner_Role_Segment = ' ' OR Opportunity_Owner_Role_Segment IS NULL);
-------------------------------------------------------------------------------
UPDATE Cloud_SMB_IB_Email_Apps_ALL
SET
	Account_Owner_Role_Segment='Unknown'
WHERE
	(Account_Owner_Role_Segment = ' ' OR Account_Owner_Role_Segment IS NULL)	;
----------------------------------------
---SA3
create or replace table `rax-abo-72-dev`.sales.cloud_smb_ib_email_apps_all as
SELECT
	Cloud_Account,Email_Account_Num, Cloud_Account_Name,Email_Account_Type,Email_Core_Account_Num, CLOUD_Desired_Billing_Date, First_Invoice_Month, First_Invoiced_Date, trunc(CAST(ifnull(First_Invoice_Amount,0) as numeric),2) AS First_Invoice_Amount, CLOUD_Account_Status, Cloud_Account_Create_Date, OPPORTUNITY_ID, Close_Date, ACCOUNTID, Account_Owner, Account_Owner_Is_Active, Account_Owner_Role, Account_Owner_Role_Segment, Account_Owner_ID, Account_Num, Account_Type, Account_Sub_Type, Opportunity_Type, Opportunity_Name, Opportunity_Owner, Opportunity_Owner_Role, Opportunity_Owner_Role_Segment, Opportunity_Owner_ID, Opportunity_Owner_Is_Active, Opp_ISDELETED, Split_Category, Split_Percentage, Cloud_Username, Category, Final_Opportunity_Type, Opportunity_Sub_Type, Record_Type, StageName, ON_DEMAND_RECONCILED, Solution_Engineer, Solution_Engineer_ID, Solution_Engineer_Active, Additional_Solution_Engineer, Additional_Solution_Engineer_ID, Additional_Sales_Rep,Additional_Sales_Rep_ID,	Additional_Sales_Rep_Is_Active,Additional_Solution_Engineer_Active, Is_Linked_Account, Is_Consolidated_Billing, Is_Internal_Account,Max_Lead_Generator_ID, Lead_Date_Passed, Reseller_Partner_Account,Partner_Account, Partner_Role, Channel, Commission_Referral_Type, Mail_1st_Month_Invoice,	Mail_Activation,CURRENCYISOCODE, APPROVAL_AMOUNT_Converted, Cloud_Account_Source, As_of_Date, Territory, LPID, PLATFORM, PLATFORM_SUB_CATEGORY, BUCKET_INFLUENCE, BUCKET_SOURCE, LEADSOURCE, FOCUS_AREA, Partner_Role_Role, Partner_Role_Name, Partner_Role_StatusX, MARKETING_SOURCED
--INTO	Cloud_SMB_IB_Email_Apps_ALL
FROM
	Cloud_SMB_IB_Email_Apps_ALL A
INNER JOIN
	SA3Invoices C
ON A.Cloud_Account=C.ACT_AccountID
WHERE
	ifnull( LOWER(Cloud_Account_Source),'cloud unknown')<>'cloud unknown'
AND LOWER(Cloud_Account_Status) not in ('approval denied');
--------------------------------------------	
---Rackspace Indirect
INSERT INTO
	`rax-abo-72-dev`.sales.cloud_smb_ib_email_apps_all
SELECT
	Cloud_Account,Email_Account_Num, Cloud_Account_Name,Email_Account_Type,Email_Core_Account_Num, CLOUD_Desired_Billing_Date, First_Invoice_Month, First_Invoiced_Date, trunc(CAST(ifnull(First_Invoice_Amount,0) as numeric),2) AS First_Invoice_Amount, CLOUD_Account_Status, Cloud_Account_Create_Date, OPPORTUNITY_ID, Close_Date, ACCOUNTID, Account_Owner, Account_Owner_Is_Active, Account_Owner_Role, Account_Owner_Role_Segment, Account_Owner_ID, Account_Num, Account_Type, Account_Sub_Type, Opportunity_Type, Opportunity_Name, Opportunity_Owner, Opportunity_Owner_Role, Opportunity_Owner_Role_Segment, Opportunity_Owner_ID, Opportunity_Owner_Is_Active, Opp_ISDELETED, Split_Category, Split_Percentage, Cloud_Username, Category, Final_Opportunity_Type, Opportunity_Sub_Type, Record_Type, StageName, ON_DEMAND_RECONCILED, Solution_Engineer, Solution_Engineer_ID, Solution_Engineer_Active, Additional_Solution_Engineer, Additional_Solution_Engineer_ID, Additional_Sales_Rep,Additional_Sales_Rep_ID,	Additional_Sales_Rep_Is_Active,Additional_Solution_Engineer_Active, Is_Linked_Account, Is_Consolidated_Billing, Is_Internal_Account,Max_Lead_Generator_ID, Lead_Date_Passed, Reseller_Partner_Account,Partner_Account, Partner_Role, Channel, Commission_Referral_Type, Mail_1st_Month_Invoice,Mail_Activation,CURRENCYISOCODE,APPROVAL_AMOUNT_Converted,Cloud_Account_Source, As_of_Date, Territory, LPID, PLATFORM, PLATFORM_SUB_CATEGORY, BUCKET_INFLUENCE, BUCKET_SOURCE, LEADSOURCE, FOCUS_AREA, Partner_Role_Role, Partner_Role_Name, Partner_Role_StatusX, MARKETING_SOURCED
FROM
	Cloud_SMB_IB_Email_Apps_ALL A
INNER JOIN
	OracleInvoices C
ON A.Email_Core_Account_Num=C.ACT_AccountID
WHERE
	ifnull( LOWER(Cloud_Account_Source),'cloud unknown')<>'cloud unknown';
-----------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_ib_email_apps_all
Set
	category='Upgrade'
WHERE
	(First_Invoice_Month <> CurrentTime_Month);
-----------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_ib_email_apps_all
Set
	category='New Footprint'
WHERE
	(First_Invoice_Month = CurrentTime_Month)
AND LOWER(category) not in  ('new footprint', 'new logo');
-----------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_ib_email_apps_all
Set
	category='New Footprint'
WHERE
	(First_Invoice_Month = CurrentTime_Month )
AND LOWER(category) in ('new');


END;