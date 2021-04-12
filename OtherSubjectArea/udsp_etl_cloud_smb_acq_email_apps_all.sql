create or replace procedure `rax-abo-72-dev`.sales.udsp_etl_cloud_smb_acq_email_apps_all(v_date date)
begin
---------------------------------------------------------------------------------------------------------------------

DECLARE CurrentMonthYear  datetime;
DECLARE CurrentSH_Date int64;
DECLARE CurrentTime_Month int64;
DECLARE FirstdayofNextMonth  datetime;
DECLARE MaxSH_Date int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;
-------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=cast(v_date as datetime);
SET FirstdayofNextMonth=`rax-abo-72-dev`.bq_functions.udf_firstdayofnextmonth(CurrentMonthYear);
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);
SET MaxSH_Date =(SELECT MAX(Time_Month_Key) FROM `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy);
SET CurrentSH_Date=(ifnull((	SELECT MAX(Time_Month_Key) 
								FROM `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy 
								WHERE Time_Month_Key= `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear))
							,MaxSH_Date)
					);
-----------------------------------------------------------------------------------------------------------------
--if exists (select * from dbo.sysobjects where id = object_id(N'Sales.dbo.Cloud_SMB_ACQ_Email_Apps_ALL') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
--drop table Sales.dbo.Cloud_SMB_ACQ_Email_Apps_ALL
-----------------------------------------------------------------------------------------------------------------
--USED FOR ACQ
create or replace temp table Commission as
SELECT
	ifnull(B.Email_Account_Num,B.DDI)														AS DDI,
	Cloud_Account_Source,
	Email_Account_Type,
	Email_Core_Account_Num,
	OPPORTUNITY_ID																			AS OPPORTUNITY_ID,
	Close_Date																				AS First_Closed_Date,
	Cloud_Account_Create_Date																AS Cloud_Account_Create_Date,
	ifnull(Is_Consolidated_Billing,0)														AS Is_Consolidated_Billing,
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_Date)						AS First_Closed_Date_Month,
	CAST('1900-01-01'	 as datetime)															AS First_Billing_Date,
	CAST(190001 AS int64)																		AS First_Billing_Month,
	Mail_1st_Month_Invoice,
	--Mail_Reseller_1st_Month_Invoice,
	Mail_Activation
--INTO	#Commission
FROM
(
SELECT DISTINCT
	A.DDI,
	MIN(Close_Date)  AS First_Closed_Date
FROM  
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot A
INNER JOIN
	`rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy B
ON A.Opportunity_Owner_Role=B.Reporting_Team
WHERE
	lower(Final_Opportunity_Type) ='mail'
AND	Opp_ISDELETED ='N'
AND Time_Month_Key=CurrentSH_Date
AND lower(STAGENAME) = ('Closed Won')
AND B.Is_Active=1
AND lower(ACQ_OR_IB) in ('acq','acq_ib_both')
AND (lower(business_unit) like '%commercial%' or lower(business_unit) like '%mail%' or lower(business_unit) like '%latam%' or lower(business_unit)  like '%marketing%' or lower(business_unit) like '%cloud office%' )
AND ifnull( lower(Cloud_Account_Source),'cloud unknown')<>'cloud unknown'
AND Close_Date	< FirstdayofNextMonth	 
GROUP BY
	DDI
)A
INNER JOIN
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot B
ON A.First_Closed_Date=B.Close_Date
AND A.DDI=B.DDI
INNER JOIN
	`rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy C
ON B.Opportunity_Owner_Role=C.Reporting_Team
WHERE
	lower(Final_Opportunity_Type) ='mail'
AND	Opp_ISDELETED ='N'
AND Time_Month_Key=CurrentSH_Date
AND lower(STAGENAME) = ('Closed Won')
AND C.Is_Active=1
AND lower(ACQ_OR_IB) in ('acq','acq_ib_both')
--AND (Business_Unit Like '%Acquisition%' OR Business_Unit like '%Mail%' OR Business_Unit Like '%LATAM%' OR Business_Unit Like '%Marketing%')
AND (lower(business_unit) like '%commercial%' or lower(business_unit) like '%mail%' or lower(business_unit) like '%latam%' or lower(business_unit)  like '%marketing%' or lower(business_unit) like '%cloud office%' )
AND ifnull(lower(Cloud_Account_Source),'cloud unknown')<>'cloud unknown'
AND Close_Date	< FirstdayofNextMonth	; 
---------------------------------------------------------------------------------------------------------------
create or replace temp table Dupes as
SELECT * --INTO	#Dupes
FROM  Commission 
where  DDI 
IN 
(
SELECT DISTINCT DDI
FROM
(
SELECT
	DDI,
	COUNT(DDI) AS Count
FROM Commission
GROUP BY DDI
HAVING COUNT(DDI) >1
)A
);
---------------------------------------------------------------------------------------------------------------
DELETE FROM Commission WHERE DDI in (SELECT ddi from Dupes);
---------------------------------------------------------------------------------------------------------------
Insert into Commission
SELECT
	A.DDI,
	Cloud_Account_Source,
	Email_Account_Type,
	Email_Core_Account_Num,
	OPPORTUNITY_ID,
	First_Closed_Date,
	Cloud_Account_Create_Date,
	Is_Consolidated_Billing,
	First_Closed_Date_Month,
	First_Billing_Date,
	First_Billing_Month,
	Mail_1st_Month_Invoice,
	Mail_Activation
FROM
(
SELECT 
	DDI,
	MIN(OPPORTUNITY_ID) Min_Opp
FROM Dupes
GROUP BY
	DDI
)A
INNER JOIN
	Dupes B
ON A.DDI=B.DDI
AND A.Min_Opp=B.OPPORTUNITY_ID;
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
FROM
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info A  
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
--Oracle
create or replace temp table OracleInvoices as 
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
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info  A  
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
create or replace temp table OracleMailInvoicing as 
SELECT 
	Account_Number					AS ACT_AccountID,
	Time_Full_Date					AS Inovice_Date
--INTO	#OracleMailInvoicing
FROM 
	`rax-datamart-dev`.corporate_dmart.fact_revenue A 
INNER JOIN
	`rax-datamart-dev`.corporate_dmart.dim_account B 
ON	A.Account_KEY = B.Account_KEY
INNER JOIN
	`rax-datamart-dev`.corporate_dmart.dim_revenue_type C 
ON	A.Revenue_Type_KEY = C.Revenue_Type_KEY
INNER JOIN
	`rax-datamart-dev`.corporate_dmart.dim_product D 
ON	A.Product_Key = D.Product_Key
INNER JOIN
	`rax-datamart-dev`.corporate_dmart.dim_time E 
ON	A.Time_Posted_KEY = E.Time_Key
WHERE
	 A.Revenue_Type_KEY in (19)
AND lower(product_group)  in ('rackspace email','managed exchange','noteworthy', 'hosted apps');
---------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
---non Rackspace
update  Commission c
Set 
	c.First_Billing_Month = ifnull(Invoiced_Date_Time_Month,0)
from
	Commission A
INNER JOIN 
	(
Select 
	z.ACT_AccountID, 
	Invoiced_Date_Time_Month, 
	TotalPrice						as Invoiced,
	TotalInvoicePrice				AS TotalInvoicePrice
from 
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info z
inner join 
(
Select 
	ACT_AccountID, 
	MIN(Invoiced_Date_Time_Month) as FirstFullInoviceMonth
from 
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info a	
inner join 
	Commission b 
on a.ACT_AccountID = b.DDI 
and a.Invoiced_Date_Time_Month >= b.First_Closed_Date_Month
WHERE
	upper(Invoice_Source) IN ('SA3')
AND upper(Email_Account_Type)<>'RACKSPACE INDIRECT'
group by 
	ACT_AccountID
)x 
on z.Invoiced_Date_Time_Month = x.FirstFullInoviceMonth 
and z.ACT_AccountID = x.ACT_AccountID
WHERE
	 upper(Invoice_Source) IN ('SA3')
)FirstInvoice
ON	A.DDI = FirstInvoice.ACT_AccountID
WHERE upper(A.Email_Account_Type)<>'RACKSPACE INDIRECT';
---------------------------------------------------------------------------------------------------------------
---Rackspace
update  Commission c
Set 
	c.First_Billing_Month = ifnull(Invoiced_Date_Time_Month,0)
from
	Commission A
INNER JOIN 
	(
Select 
	z.ACT_AccountID, 
	Invoiced_Date_Time_Month, 
	TotalPrice						as Invoiced,
	TotalInvoicePrice				AS TotalInvoicePrice
from 
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info z
inner join 
(
Select 
	ACT_AccountID, 
	MIN(Invoiced_Date_Time_Month) as FirstFullInoviceMonth
from 
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info a	
inner join 
	Commission b 
on a.ACT_AccountID = b.Email_Core_Account_Num 
and a.Invoiced_Date_Time_Month >= b.First_Closed_Date_Month
WHERE
	upper(Email_Account_Type)<>'RACKSPACE INDIRECT'
AND upper(Invoice_Source) IN ('ORACLE')
group by 
	ACT_AccountID
)x 
on z.Invoiced_Date_Time_Month = x.FirstFullInoviceMonth 
and z.ACT_AccountID = x.ACT_AccountID
WHERE
	upper(Invoice_Source) IN ('ORACLE')
)FirstInvoice
ON	A.Email_Core_Account_Num = FirstInvoice.ACT_AccountID
WHERE
	upper(A.Email_Account_Type)<>'RACKSPACE INDIRECT';
---------------------------------------------------------------------------------------------------------------
---Non Rackspace Indirect
update  Commission C
Set 
	C.First_Billing_Date = ifnull(`rax-abo-72-dev`.bq_functions.udfdatepart(firstfullinovicedate),DATE('1900-01-01'))	
from 
	Commission A
INNER JOIN 
	(
Select 
	accountNumber			AS ACT_AccountID, 
	FirstFullInoviceDate
from 
	`rax-abo-72-dev`.mailtrust.adminvoices z
inner join 
(
Select 
	accountNumber			AS ACT_AccountID, 
	MIN(invoiceDate)		as FirstFullInoviceDate
from 
	`rax-abo-72-dev`.mailtrust.adminvoices a	
inner join 
	Commission b 
on a.accountNumber = b.DDI 
and `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(a.invoiceDate) >= `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(b.First_Closed_Date)
WHERE
	LOWER(Email_Account_Type)<>'rackspace indirect'
group by 
	accountNumber
)x 
on z.invoiceDate = x.FirstFullInoviceDate 
and z.accountNumber = x.ACT_AccountID
)FirstInvoice
ON	A.DDI = FirstInvoice.ACT_AccountID
WHERE
	LOWER(A.Email_Account_Type)<>'rackspace indirect';
---------------------------------------------------------------------------------------------------------------
--Rackspace Indirect
update  Commission C
Set 
	C.First_Billing_Date = CAST(ifnull(`rax-abo-72-dev`.bq_functions.udfdatepart(firstfullinovicedate),DATE('1900-01-01'))	 AS DATETIME)
from 
	Commission A
INNER JOIN 
	(
Select 
	z.ACT_AccountID, 
	FirstFullInoviceDate
from 
	OracleMailInvoicing z
inner join 
(
Select 
	ACT_AccountID			AS ACT_AccountID, 
	MIN(Inovice_Date)		AS FirstFullInoviceDate
from 
	OracleMailInvoicing a	
inner join 
	Commission b 
on a.ACT_AccountID = b.Email_Core_Account_Num 
and `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(a.Inovice_Date) >= `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(b.First_Closed_Date)
WHERE
	LOWER(Email_Account_Type)<>'rackspace indirect'
group by 
	ACT_AccountID
)x 
on z.Inovice_Date = x.FirstFullInoviceDate 
and z.ACT_AccountID = x.ACT_AccountID
)FirstInvoice
ON	A.Email_Core_Account_Num = FirstInvoice.ACT_AccountID
WHERE
	LOWER(A.Email_Account_Type)<>'rackspace indirect';
-------------------------------------------------------------------------------------
---Update Second_Billing_Month and Second_Month_Invoice_Total
-------------------------------------------------------------------------------------
--ACQ Commissions
create or replace table `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all as
SELECT DISTINCT
	A.DDI										AS Cloud_Account,
	Email_Account_Num							AS Email_Account_Num, 
	Account_Name								AS Cloud_Account_Name,
	A.Email_Account_Type,
	A.Email_Core_Account_Num,
	CLOUD_Desired_Billing_Date,
	CLOUD_Account_Status,
	Account_Status_Online_ID					AS Cloud_Account_Status_Online_ID,
	A.Cloud_Account_Create_Date,
	Cloud_Account_Tenure,
	Close_Date,
	First_Closed_Date_Month,
	First_Billing_Date,
	First_Billing_Month,
	First_Invoice_Month,
	First_Invoiced_Date,
	First_Invoice_Amount,
	A.OPPORTUNITY_ID,
	Master_OPPORTUNITY_ID,	
	ACCOUNTID,
	Account_Owner,
	Account_Owner_Is_Active,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Owner_ID,
	Account_Num,
	Account_Type,
	Account_Sub_Type,
	Opportunity_Type,
	Billing_Name								AS Opportunity_Name,
	Opportunity_Owner,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opportunity_Owner_ID,
	Opportunity_Owner_Is_Active,
	Opp_ISDELETED,
	Cloud_Username,
	Split_Category								AS Split_Category,
	Split_Percentage							AS Split_Percentage,
	Final_Opportunity_Type						AS Final_Opportunity_Type,
	Category									AS Category,
	Opportunity_Sub_Type,
	Record_Type,
	'Closed Won'								AS StageName,
	'TRUE'										AS ON_DEMAND_RECONCILED,
	Solution_Engineer_ID,
	Solution_Engineer,
	Solution_Engineer_Is_Active					AS Solution_Engineer_Active,
	Additional_Solution_Engineer,
	Additional_Solution_Engineer_ID,
	Additional_Solution_Engineer_Is_Active		AS Additional_Solution_Engineer_Active,	
	Additional_Sales_Rep,
	Additional_Sales_Rep_ID,
	Additional_Sales_Rep_Is_Active,					
	LEAD_GENERATOR_ID,
	LEAD_GENERATOR_ROLE,
	LEAD_DATE_PASSED,
	MAX_LEAD_GENERATOR_ID,
	MAX_LEAD_GENERATOR,
	MAX_LEAD_ROLE,
	MAX_DATE_PASSED,		
	Reseller_Partner_Account,
	Partner_Account,
	Partner_Role,
	Channel,	
	CAST('' AS string)						AS Commission_Referral_Type,
	CASE	
		WHEN 
			lower(A.Cloud_Account_Source)='cloud uk'
		THEN
			'GBP'
		ELSE
			'USD'
	END						AS 	CURRENCYISOCODE,
	APPROVAL_AMOUNT_Converted,
	Is_Linked_Account,
	B.Is_Consolidated_Billing,
	Is_Internal_Account,
	Mail_1st_Month_Invoice,
	--Mail_Reseller_1st_Month_Invoice,
	Mail_Activation,
	A.Cloud_Account_Source,
	`rax-abo-72-dev`.bq_functions.udfdatepart(current_date())		AS As_of_Date,
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
	Commission A
INNER JOIN
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot B
ON A.OPPORTUNITY_ID=B.OPPORTUNITY_ID
AND A.DDI=B.DDI
INNER JOIN
	SA3Invoices C
ON A.DDI=C.ACT_AccountID
WHERE
	ifnull( lower(A.Cloud_Account_Source),'cloud unknown')<>'cloud unknown'
AND lower(A.Email_Account_Type)<>'rackspace indirect';
------------------------------
INSERT INTO 	`rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SELECT DISTINCT
	A.DDI										AS Cloud_Account,
	Email_Account_Num							AS Email_Account_Num, 
	Account_Name								AS Cloud_Account_Name,
	A.Email_Account_Type,
	A.Email_Core_Account_Num,
	CLOUD_Desired_Billing_Date,
	CLOUD_Account_Status,
	Account_Status_Online_ID					AS Cloud_Account_Status_Online_ID,
	A.Cloud_Account_Create_Date,
	Cloud_Account_Tenure,
	Close_Date,
	First_Closed_Date_Month,
	First_Billing_Date,
	First_Billing_Month,
	First_Invoice_Month,
	First_Invoiced_Date,
	First_Invoice_Amount,
	A.OPPORTUNITY_ID,
	Master_OPPORTUNITY_ID,	
	ACCOUNTID,
	Account_Owner,
	Account_Owner_Is_Active,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Owner_ID,
	Account_Num,
	Account_Type,
	Account_Sub_Type,
	Opportunity_Type,
	Billing_Name								AS Opportunity_Name,
	Opportunity_Owner,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opportunity_Owner_ID,
	Opportunity_Owner_Is_Active,
	Opp_ISDELETED,
	Cloud_Username,
	Split_Category								AS Split_Category,
	Split_Percentage							AS Split_Percentage,	
	Final_Opportunity_Type						AS Final_Opportunity_Type,
	Category									AS Category,
	Opportunity_Sub_Type,
	Record_Type,
	'Closed Won'								AS StageName,
	'TRUE'										AS ON_DEMAND_RECONCILED,
	Solution_Engineer_ID,
	Solution_Engineer,
	Solution_Engineer_Is_Active					AS Solution_Engineer_Active,
	Additional_Solution_Engineer,
	Additional_Solution_Engineer_ID,
	Additional_Solution_Engineer_Is_Active		AS Additional_Solution_Engineer_Active,	
	Additional_Sales_Rep,
	Additional_Sales_Rep_ID,
	Additional_Sales_Rep_Is_Active,						
	LEAD_GENERATOR_ID,
	LEAD_GENERATOR_ROLE,
	LEAD_DATE_PASSED,
	MAX_LEAD_GENERATOR_ID,
	MAX_LEAD_GENERATOR,
	MAX_LEAD_ROLE,
	MAX_DATE_PASSED,		
	Reseller_Partner_Account,
	Partner_Account,
	Partner_Role,
	Channel,	
	CAST(' ' as string)					AS Commission_Referral_Type,
	CASE	
		WHEN 
			lower(A.Cloud_Account_Source)='Cloud UK'
		THEN
			'GBP'
		ELSE
			'USD'
	END						AS 	CURRENCYISOCODE,
	APPROVAL_AMOUNT_Converted,
	Is_Linked_Account,
	B.Is_Consolidated_Billing,
	Is_Internal_Account,
	Mail_1st_Month_Invoice,
	Mail_Activation,	
	A.Cloud_Account_Source,
	`rax-abo-72-dev`.bq_functions.udfdatepart(current_date())		AS As_of_Date,
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
FROM   Commission A
INNER JOIN
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot B
ON A.OPPORTUNITY_ID=B.OPPORTUNITY_ID
INNER JOIN
	OracleInvoices C
ON A.Email_Core_Account_Num=C.ACT_AccountID
WHERE
	ifnull( lower(A.Cloud_Account_Source),'cloud unknown')<>'cloud unknown'
AND lower(A.Email_Account_Type)='rackspace indirect';
---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------
--Internal,Employee,Is_Internal_Account
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	c
SET 
	c.Opportunity_Type= ' '
WHERE
	Opportunity_Type IS  NULL OR Opportunity_Type='0';
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	 c
SET 
	c.Opportunity_Owner_ID= '0'
WHERE
	Opportunity_Owner_ID=' ' OR Opportunity_Owner_ID='0';
-----------------------------------------------------	
UPDATE  `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Opportunity_Owner_Is_Active= 'true'
WHERE
	Opportunity_Owner_ID='0';
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Opp_ISDELETED= 'N'
WHERE
	lower(Opp_ISDELETED)='unknown' ;
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Solution_Engineer= ' '
WHERE
	Solution_Engineer IS  NULL OR Solution_Engineer='0';
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Solution_Engineer= ' '
WHERE
	lower(Solution_Engineer)='unknown'	;
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Solution_Engineer_ID= ' '
WHERE
	Solution_Engineer=' '	;
-----------------------------------------------------		
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Solution_Engineer_Active= ' '
WHERE
	Solution_Engineer=' ';	
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Additional_Solution_Engineer= ' '
WHERE
	Additional_Solution_Engineer IS  NULL OR Additional_Solution_Engineer='0';
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Additional_Solution_Engineer= ' '
WHERE
	lower(Additional_Solution_Engineer)='unknown'	;
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Additional_Solution_Engineer_ID= ' '
WHERE
	Additional_Solution_Engineer=' '	;
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Additional_Solution_Engineer_Active= ' '
WHERE
	Additional_Solution_Engineer=' '	;
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Max_Lead_Generator_ID= ' '
WHERE
	Max_Lead_Generator_ID IS  NULL OR Max_Lead_Generator_ID='0';
-----------------------------------------------------	
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET 
	Reseller_Partner_Account= Partner_Account
WHERE
	Reseller_Partner_Account IS NULL OR Reseller_Partner_Account=' ';
--------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET
	Channel='Referral',
	Commission_Referral_Type='Partner Referral'	
WHERE
	(Partner_Account <> ' ' AND ifnull(Partner_Account,'0')<> '0');
---------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all	
SET
	Partner_Account=' ',
	Partner_Role=' ',
	Channel=' ',
	Commission_Referral_Type=' '	
WHERE  
	( lower(Account_Sub_Type)= 'reseller' AND lower(Account_Type)='customer');
---------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all
SET
	Partner_Account=' ',
	Partner_Role=' '
WHERE
	Partner_Account IS  NULL OR Partner_Account='0';
-----------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all
Set
	category='Upgrade'
WHERE
	(First_Invoice_Month <> CurrentTime_Month);
-----------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all
Set
	category='New Footprint'
WHERE
	(First_Invoice_Month = CurrentTime_Month)
AND lower(category) NOT IN  ('New Footprint', 'New Logo');
-----------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all
Set
	category='New Footprint'
WHERE
	(First_Invoice_Month = CurrentTime_Month)
AND lower(category) IN ('new');
-----------------------------------------------------------------------------------------------------------------
end;