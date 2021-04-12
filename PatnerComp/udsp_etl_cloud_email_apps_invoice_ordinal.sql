CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.net_revenue.udsp_etl_cloud_email_apps_invoice_ordinal()
BEGIN

create or replace table `rax-abo-72-dev`.net_revenue.cloud_email_apps_invoice_ordinal AS
SELECT DISTINCT 
	Account_Key,
	Account,
	Time_Month_Key,
	ROW_NUMBER() OVER(PARTITION BY Account ORDER BY Time_Month_Key ASC)	AS Invoice_Ordinal
FROM
	(
	SELECT DISTINCT --INTO    #DATA
		 concat(accountNumber,"",'Cloud_Email_Apps')		AS Account_Key,
		accountNumber												AS Account,
		`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(invoiceDate)						AS Time_Month_Key
	FROM
		(
			SELECT * --INTO	#Invoice_satage
			FROM
			(
			SELECT DISTINCT
				accountNumber,
				invoiceDate
			FROM
				`rax-abo-72-dev`.mailtrust.adminvoices  
			WHERE
				LOWER(invoiceType) IN ('signup', 'billing')  
			AND void <> 1   
			AND totalDue >0 )
		) --#Invoice_satage
	) A --#DATA A
;

END;