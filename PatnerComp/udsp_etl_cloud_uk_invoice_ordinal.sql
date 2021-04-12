CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.cloud_uk.udsp_etl_cloud_uk_invoice_ordinal()
BEGIN

CREATE OR REPLACE TABLE `rax-abo-72-dev`.cloud_uk.cloud_uk_invoiced_w_ordinal AS 
SELECT
	Account_Key,
	A.Account,
	TotalPrice,
	A.Time_Month_Key,
	Invoice_Ordinal
FROM
	(
	SELECT DISTINCT --INTO	#Invoice_Data
	DDI											AS Account,
	SUM(Charge_Amount)							AS TotalPrice,
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Invoice_Date)	AS Time_Month_Key
	FROM
		`rax-abo-72-dev`.cloud_uk.cloud_invoices A
	GROUP BY
		DDI,
		`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Invoice_Date)
	) A --#Invoice_Data A
INNER JOIN
	(
		SELECT DISTINCT --INTO	#Cloud_UK_Invoice_Ordinal
			Account_Key,
			Account,
			Time_Month_Key,
			ROW_NUMBER() OVER(PARTITION BY Account ORDER BY Time_Month_Key ASC)	AS Invoice_Ordinal
		FROM
			(
			SELECT DISTINCT  --INTO	#DATA
			Cloud_Account_Key									AS Account_Key,
			DDI													AS Account,
			`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Invoice_Date)			AS Time_Month_Key
			FROM `rax-abo-72-dev`.cloud_uk.cloud_invoices A
			) A --#DATA A
	) B --#Cloud_UK_Invoice_Ordinal B
ON A.Account=B.Account
AND A.Time_Month_Key=B.Time_Month_Key
ORDER BY
	Account,
	Invoice_Ordinal;

end;