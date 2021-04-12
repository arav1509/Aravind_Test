CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_cloud_apps_invoiced()
begin

create or replace temp table Max_invoiced as
SELECT--INTO	#Max_invoiced	
	AccountNumber		AS Account,
	MAX(invoiceDate)	AS Max_Invoice_Date
FROM
	`rax-abo-72-dev`.mailtrust.adminvoices 
WHERE
    lower(invoiceType) IN ('signup', 'billing','manual')  
AND void <> 1   
AND totalDue >0
GROUP BY
    AccountNumber;
--------------------------------------------------------------------------------------------------------------
create or replace temp table MAX_Invoice_DATA as   
SELECT --INTO    #MAX_Invoice_DATA
   `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(invoiceDate)		AS Invoiced_TMK,
    MAX(invoiceDate)						AS MAX_Invoice_Date_by_Month
FROM
	`rax-abo-72-dev`.mailtrust.adminvoices  A  
WHERE
	lower(invoiceType) IN ('signup', 'billing','manual')  
AND void <> 1   
AND totalDue >0
GROUP BY `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(invoiceDate);
create or replace temp table Apps_Billing_Accounts as 
SELECT DISTINCT 
	DDI
--INTO	#Apps_Billing_Accounts
FROM (
select  distinct 
   ifnull(A.Email_Account_Num,A.DDI)	AS DDI
 FROM
   `rax-landing-qa`.salesforce_ods.qaccounts A 
WHERE  
    ifnull(A.Email_Account_Num,A.DDI) IS NOT NULL 
AND DELETE_FLAG='N'
AND lower(A.TYPEX) in ('mailtrust customer')
);

create or replace temp table Invoices as 
SELECT DISTINCT
	CAST(A.accountNumber as string)		AS ACT_AccountID,
	'Invoice'								AS Charge_Type,
		(
		SELECT
			MAX(AAA.paymentInterval)
		FROM
			`rax-abo-72-dev`.mailtrust.admcompanies AAA, `rax-abo-72-dev`.mailtrust.adminvoices Adm 
		LEFT OUTER JOIN
			 Max_invoiced B
		ON AAA.accountNumber=B.account
		WHERE
			Adm.accountNumber=AAA.accountNumber
		AND CAST (`rax-abo-72-dev`.bq_functions.udfdatepart(Adm.InvoiceDate) as datetime)  >= ifnull(AAA.invoiceDate,Max_Invoice_Date)
		group by
			 ifnull(AAA.invoiceDate,Max_Invoice_Date)
		--order by			 ifnull(AAA.invoiceDate,Max_Invoice_Date) DESC
       limit 1
	) as paymentInterval,
	(
	SELECT 
			MAX(AAA.paymentInterval)
		FROM
			`rax-abo-72-dev`.mailtrust.admcompanies AAA ,`rax-abo-72-dev`.mailtrust.adminvoices  adm
		LEFT OUTER JOIN
			 Max_invoiced B
		ON AAA.accountNumber=B.account
		WHERE
			adm.accountNumber=AAA.accountNumber
		AND	CAST (`rax-abo-72-dev`.bq_functions.udfdatepart(adm.invoiceDate)	As Datetime) <= ifnull(AAA.invoiceDate,Max_Invoice_Date)
		group by
			 ifnull(AAA.invoiceDate,Max_Invoice_Date)
		--order by			 ifnull(AAA.invoiceDate,Max_Invoice_Date) ASC 
       limit 1
	) as paymentInterval2,
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(invoiceDate) AS Invoiced_Date_Time_Month_Key,
	CAST (`rax-abo-72-dev`.bq_functions.udfdatepart(invoiceDate) as datetime)		AS Invoiced_Date,
	CAST(totalDue as numeric)							AS TotalPrice,
	CAST(totalDue as numeric)							AS TotalInvoicePrice
--INTO	#Invoices 
FROM	Apps_Billing_Accounts BA
INNER JOIN
	`rax-abo-72-dev`.mailtrust.adminvoices A  
ON BA.ddi=CAST(A.accountNumber as string)
WHERE
	lower(invoiceType) IN ('signup', 'billing','manual')  
AND void <> 1   
AND totalDue >0;


create or replace table `rax-abo-72-dev`.sales.cloud_apps_invoiced as
SELECT
	ACT_AccountID,
	CAST(concat(RTRIM(CAST(ACT_AccountID as string)),'','Cloud_Email_Apps') as string)  AS Account_Key,
	CAST('Invoice' as string)											  AS Charge_Type,
	ifnull(ifnull(paymentInterval,paymentInterval2),1)							  AS Payment_Interval,
	MIN(Invoiced_Date)														  AS First_Invoiced_Date_Time_Month,
	Invoiced_Date_Time_Month_Key												  AS Invoiced_Date_Time_Month_Key,
	0																	  AS Invoice_Ordinal,
	CAST(SUM(TotalPrice) as numeric)											  AS TotalPrice,	
	CAST(SUM(TotalPrice) as numeric)											  AS TotalInvoicePrice,
	CAST(0	as numeric)														  AS TotalPrice_div_Pay_Interval,
	CAST('SA3' as string)												  AS Invoice_Source,
	MAX_Invoice_Date_by_Month												  AS MAX_Invoice_Date_by_Month,
	CAST('1900-01-01'as datetime)												  AS AS_Of_Date	

FROM Invoices A
LEFT OUTER JOIN
    MAX_Invoice_DATA  B
ON A.Invoiced_Date_Time_Month_Key=B.Invoiced_TMK
GROUP BY
    ACT_AccountID,
    CAST(concat(RTRIM(CAST(ACT_AccountID as string)),'','Cloud_Email_Apps') as string),
    ifnull(ifnull(paymentInterval,paymentInterval2),1),
    Invoiced_Date_Time_Month_Key,
    MAX_Invoice_Date_by_Month	;
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_apps_invoiced
SET
	Payment_Interval=1
WHERE
	Payment_Interval=0
;
------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_apps_invoiced
SET
	TotalPrice_div_Pay_Interval=(TotalPrice/Payment_Interval)
where true;
---------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.cloud_apps_invoiced
SELECT
	Cloud_Account										AS ACT_AccountID,
	CAST(concat(Cloud_Account,'','Cloud_Email_Apps') as string)	AS Account_Key,
	'Billing Adjustment'								AS Charge_Type,
	Payment_Interval,
	Invoice_Date										AS First_Invoiced_Date_Time_Month,
	Invoice_Month_Key									AS Invoiced_Date_Time_Month_Key,
	0												AS Invoice_Ordinal,
	Adjustment										AS TotalPrice,
	Adjustment										AS TotalInvoicePrice,
	CAST(0	as numeric)									AS TotalPrice_div_Pay_Interval,
	Invoice_Source										AS Invoice_Source,
	MAX_Invoice_Date_by_Month						     AS MAX_Invoice_Date_by_Month,
	current_date()							AS AS_Of_Date
FROM
  `rax-abo-72-dev`.sales.salesforce_cloud_apps_billing_adjustments A
LEFT OUTER JOIN
    MAX_Invoice_DATA  B
ON A.Invoice_Month_Key=B.Invoiced_TMK;
------------------------------------------------------------------------------------------------------------
create or replace temp table SA3Ord as
SELECT DISTINCT
	ACT_AccountID,
	Invoiced_Date_Time_Month_Key,
	Invoice_Source								AS Invoice_Source,
	Charge_Type
--INTO	#SA3Ord		
FROM
	`rax-abo-72-dev`.sales.cloud_apps_invoiced A  
WHERE
	lower(Invoice_Source)='sa3'
and lower(Charge_Type)='invoice';
------------------------------------------------------------------------------------------------------------
create or replace temp table SA3_Ordinal as
SELECT DISTINCT
	ACT_AccountID,
	Invoiced_Date_Time_Month_Key,
	ROW_NUMBER() OVER(PARTITION BY ACT_AccountID ORDER BY Invoiced_Date_Time_Month_Key ASC) 
												AS Invoice_Ordinal,
	Invoice_Source								AS Invoice_Source,
	Charge_Type
--INTO	#SA3_Ordinal		
FROM SA3Ord A  ;
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_apps_invoiced c
SET
	c.Invoice_Ordinal =B.Invoice_Ordinal
FROM
	`rax-abo-72-dev`.sales.cloud_apps_invoiced  A
INNER JOIN
	SA3_Ordinal B
ON A.ACT_AccountID=B.ACT_AccountID
AND A.Invoice_Source=B.Invoice_Source
AND A.Invoiced_Date_Time_Month_Key=B.Invoiced_Date_Time_Month_Key
AND A.Charge_Type=B.Charge_Type
where true;
------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_apps_invoiced c
SET
	c.TotalPrice_div_Pay_Interval=(TotalPrice/Payment_Interval)
WHERE
	lower(Charge_Type)='billing adjustment'
AND TotalPrice_div_Pay_Interval=0;
------------------------------------------------------------------------------------------------------------

UPDATE `rax-abo-72-dev`.sales.cloud_apps_invoiced c
SET
	c.First_Invoiced_Date_Time_Month=b.First_Invoiced_Date_Time_Month
FROM
	`rax-abo-72-dev`.sales.cloud_apps_invoiced A
INNER JOIN
	`rax-abo-72-dev`.sales.cloud_apps_invoiced B
ON A.ACT_AccountID=B.ACT_AccountID
WHERE
	B.Invoice_Ordinal=1
;
end;